/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use jni::objects::{JClass, JString, JByteArray};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;

use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use datafusion::DATAFUSION_VERSION;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};

use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use prost::Message;

use anyhow::Result;
use std::sync::Arc;
use tokio::runtime::Runtime;
use futures::stream::StreamExt;
use std::pin::Pin;
use arrow::record_batch::RecordBatch;
use arrow_ipc::writer::StreamWriter;

struct DataFusionContext {
    context: SessionContext,
    runtime: Arc<Runtime>,
}

// StreamWrapper to hold the DataFusion stream for JNI pointer pattern
pub struct StreamWrapper {
    // Option allows us to take the stream out when it's consumed
    stream: Option<datafusion::physical_plan::SendableRecordBatchStream>,
    runtime: Arc<Runtime>,
}

impl StreamWrapper {
    fn new(stream: datafusion::physical_plan::SendableRecordBatchStream, runtime: Arc<Runtime>) -> Self {
        Self {
            stream: Some(stream),
            runtime,
        }
    }

    // Get next batch as JSON string
    async fn next_batch_json(&mut self) -> Result<Option<String>> {
        if let Some(ref mut stream) = self.stream {
            // Use the same pattern as standalone demo - stream.next() returns Option<Result<RecordBatch, Error>>
            match stream.next().await {
                Some(Ok(batch)) => {
                    // Convert RecordBatch to JSON
                    let mut buffer = Vec::new();
                    {
                        let mut json_writer = arrow_json::ArrayWriter::new(&mut buffer);
                        json_writer.write_batches(&[&batch])?;
                        json_writer.finish()?;
                    }
                    let json_str = String::from_utf8(buffer)?;
                    Ok(Some(json_str))
                }
                Some(Err(e)) => Err(anyhow::anyhow!("Stream error: {}", e)),
                None => {
                    self.stream = None;
                    Ok(None) // End of stream
                }
            }
        } else {
            Ok(None) // Stream already consumed
        }
    }

    // Get next batch as RecordBatch (arrow pointer)
    async fn next_batch_arrow(&mut self) -> Result<Option<datafusion::arrow::record_batch::RecordBatch>> {
        if let Some(ref mut stream) = self.stream {
            // Use the same pattern as standalone demo - stream.next() returns Option<Result<RecordBatch, Error>>
            match stream.next().await {
                Some(Ok(batch)) => {
                    Ok(Some(batch))
                }
                Some(Err(e)) => Err(anyhow::anyhow!("Stream error: {}", e)),
                None => {
                    self.stream = None;
                    Ok(None) // End of stream
                }
            }
        } else {
            Ok(None) // Stream already consumed
        }
    }
}

/// Create a new DataFusion session context and register a parquet table
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionJNI_nativeCreateContext(
    mut env: JNIEnv,
    _class: JClass,
    parquet_file_path: JString,
) -> jlong {
    let parquet_path: String = match env.get_string(&parquet_file_path) {
        Ok(path) => path.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException",
                &format!("Failed to get parquet file path: {}", e));
            return 0;
        }
    };

    match create_context_with_table(&parquet_path) {
        Ok(ctx) => {
            let boxed_ctx = Box::into_raw(Box::new(ctx)) as jlong;
            boxed_ctx
        },
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException",
                &format!("Failed to create DataFusion context: {}", e));
            0
        }
    }
}

fn create_context_with_table(parquet_path: &str) -> Result<DataFusionContext> {
    let rt = Arc::new(Runtime::new()?);

    rt.block_on(async {
        let config = SessionConfig::new().with_repartition_aggregations(true);
        let context = SessionContext::new_with_config(config);

        // Register the parquet file as a table named "sample_table"
        if std::path::Path::new(parquet_path).exists() {
            // Read and print the parquet file schema
            let table_url = ListingTableUrl::parse(parquet_path)?;
            let parquet_format = ParquetFormat::default();
            let listing_options = ListingOptions::new(Arc::new(parquet_format));

            let schema = listing_options
                .infer_schema(&context.state(), &table_url)
                .await?;

            println!("Parquet file schema for '{}': {}", parquet_path, schema);

            context.register_parquet("hits", parquet_path, ParquetReadOptions::default()).await?;
        } else {
            // For now, just log a warning if the file doesn't exist
            // In production, this should be handled more gracefully
            log::warn!("Parquet file not found at: {}, continuing without table registration", parquet_path);
        }

        Ok(DataFusionContext {
            context,
            runtime: rt.clone(),
        })
    })
}

/// Close and cleanup a DataFusion context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionJNI_nativeCloseContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    if context_id != 0 {
        let _ = unsafe { Box::from_raw(context_id as *mut DataFusionContext) };
    }
}

/// Execute a Substrait query plan
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionJNI_nativeExecuteSubstraitQueryPlan(
    env: JNIEnv,
    _class: JClass,
    context_id: jlong,
    query_plan_bytes: jbyteArray,
) -> jstring {
    if context_id == 0 {
        let error_msg = "Invalid context ID";
        return match env.new_string(error_msg) {
            Ok(jstr) => jstr.as_raw(),
            Err(_) => std::ptr::null_mut(),
        };
    }

    let df_context = unsafe { &*(context_id as *const DataFusionContext) };

    // Convert Java byte array to Rust Vec<u8>
    let byte_array = unsafe { JByteArray::from_raw(query_plan_bytes) };
    let plan_bytes = match env.convert_byte_array(byte_array) {
        Ok(bytes) => bytes,
        Err(e) => {
            let error_msg = format!("Failed to convert byte array: {}", e);
            return match env.new_string(error_msg) {
                Ok(jstr) => jstr.as_raw(),
                Err(_) => std::ptr::null_mut(),
            };
        }
    };

    match execute_substrait_query(&df_context, &plan_bytes) {
        Ok(result) => {
            match env.new_string(result) {
                Ok(jstr) => jstr.as_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        },
        Err(e) => {
            let error_msg = format!("Query execution failed: {}", e);
            match env.new_string(error_msg) {
                Ok(jstr) => jstr.as_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
    }
}

/// Execute a Substrait query plan and return stream pointer (NEW VERSION)
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionJNI_nativeExecuteSubstraitQueryStream(
    env: JNIEnv,
    _class: JClass,
    context_id: jlong,
    query_plan_bytes: jbyteArray,
) -> jlong {
    if context_id == 0 {
        // Return 0 to indicate error
        return 0;
    }

    let df_context = unsafe { &*(context_id as *const DataFusionContext) };

    // Convert Java byte array to Rust Vec<u8>
    let byte_array = unsafe { JByteArray::from_raw(query_plan_bytes) };
    let plan_bytes = match env.convert_byte_array(byte_array) {
        Ok(bytes) => bytes,
        Err(_) => return 0, // Return 0 on error
    };

    match execute_substrait_query_stream(&df_context, &plan_bytes) {
        Ok(stream_ptr) => stream_ptr as jlong, // Return pointer as jlong
        Err(_) => 0, // Return 0 on error
    }
}

/// Get next batch from stream as JSON string
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionJNI_nativeNextBatch(
    env: JNIEnv,
    _class: JClass,
    stream_pointer: jlong,
) -> jstring {
    if stream_pointer == 0 {
        return std::ptr::null_mut();
    }

    let stream_wrapper = unsafe { &mut *(stream_pointer as *mut StreamWrapper) };
    let runtime = stream_wrapper.runtime.clone();

    match runtime.block_on(async {
        stream_wrapper.next_batch_json().await
    }) {
        Ok(Some(json_str)) => {
            // Return JSON string
            match env.new_string(json_str) {
                Ok(jstr) => jstr.as_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
        Ok(None) => {
            // End of stream - return null
            std::ptr::null_mut()
        }
        Err(_) => {
            // Error occurred - return null
            std::ptr::null_mut()
        }
    }
}

/// Get next batch from stream as arrow RecordBatch - returns Result<Option<RecordBatch>>
pub fn Java_org_opensearch_datafusion_DataFusionJNI_nativeNextBatchArrow(
    stream_ptr: *mut StreamWrapper
) -> Result<Option<RecordBatch>> {
    if stream_ptr.is_null() {
        return Ok(None);
    }

    let stream_wrapper = unsafe { &mut *stream_ptr };
    let runtime = stream_wrapper.runtime.clone();

    runtime.block_on(async {
        stream_wrapper.next_batch_arrow().await
    })
}

/// Close and cleanup a StreamWrapper (important for memory management)
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionJNI_nativeCloseStream(
    _env: JNIEnv,
    _class: JClass,
    stream_pointer: jlong,
) {
    if stream_pointer != 0 {
        // Reclaim ownership and let it drop automatically
        let _ = unsafe { Box::from_raw(stream_pointer as *mut StreamWrapper) };
    }
}

// Modified to return streaming results as a pointer (for JNI pattern)
fn execute_substrait_query_stream(df_context: &DataFusionContext, plan_bytes: &[u8]) -> Result<*mut StreamWrapper> {
    let runtime_clone = df_context.runtime.clone();

    let stream_wrapper = df_context.runtime.block_on(async {
        // Parse the Substrait plan from bytes
        let substrait_plan = datafusion_substrait::substrait::proto::Plan::decode(plan_bytes)?;
        println!("Loaded Substrait plan with {} relations", substrait_plan.relations.len());

        // Convert Substrait plan to DataFusion logical plan
        let logical_plan = from_substrait_plan(&df_context.context.state(), &substrait_plan).await?;
        println!("Converted to DataFusion logical plan: {:?}", logical_plan);

        // Execute logical plan and get streaming results (instead of collecting all)
        let dataframe = df_context.context.execute_logical_plan(logical_plan).await?;
        let stream = dataframe.execute_stream().await?;

        Ok::<StreamWrapper, anyhow::Error>(StreamWrapper::new(stream, runtime_clone))
    })?;

    // Convert to raw pointer for JNI
    let boxed = Box::new(stream_wrapper);
    Ok(Box::into_raw(boxed))
}

// Keep the original function for backward compatibility if needed
fn execute_substrait_query(df_context: &DataFusionContext, plan_bytes: &[u8]) -> Result<String> {
    df_context.runtime.block_on(async {
        // Parse the Substrait plan from bytes
        let substrait_plan = datafusion_substrait::substrait::proto::Plan::decode(plan_bytes)?;
        println!("Loaded Substrait plan with {} relations", substrait_plan.relations.len());

        // Convert Substrait plan to DataFusion logical plan
        let logical_plan = from_substrait_plan(&df_context.context.state(), &substrait_plan).await?;
        println!("Converted to DataFusion logical plan: {:?}", logical_plan);

        // Execute logical plan through SessionContext - create DataFrame and collect
        let dataframe = df_context.context.execute_logical_plan(logical_plan).await?;
        let batches = dataframe.collect().await?;

        // Convert results to JSON
        let mut buffer = Vec::new();
        {
            let mut json_writer = arrow_json::ArrayWriter::new(&mut buffer);
            json_writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
            json_writer.finish()?;
        }

        let json_str = String::from_utf8(buffer)?;
        Ok(json_str)
    })
}
















/// Get version information
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionJNI_getVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version_info = format!(
        "{{\"datafusion_version\": \"{}\", \"substrait_version\": \"0.50.0\"}}",
        DATAFUSION_VERSION
    );
    env.new_string(version_info).expect("Couldn't create Java string").as_raw()
}

/// Simple function to read and print parquet schema
async fn read_parquet_schema() -> Result<()> {
    let parquet_path = "/Users/pudyodu/clickbench_queries/original_data/hits.parquet";

    println!("Reading parquet schema for: {}", parquet_path);

    if std::path::Path::new(parquet_path).exists() {
        let context = SessionContext::new();

        let table_url = ListingTableUrl::parse(parquet_path)?;
        let parquet_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(parquet_format));

        let schema = listing_options
            .infer_schema(&context.state(), &table_url)
            .await?;

        println!("Schema: {}", schema);

        // Collect unique data types
        let mut unique_types = std::collections::HashSet::new();
        for field in schema.fields() {
            unique_types.insert(format!("{:?}", field.data_type()));
        }

        println!("\nUnique Arrow data types found:");
        for data_type in unique_types {
            println!("  - {}", data_type);
        }

        println!("\nTotal number of columns: {}", schema.fields().len());

        // Register the parquet table to query actual data
        context.register_parquet("hits", parquet_path, ParquetReadOptions::default()).await?;

        // First check EventDate format
        println!("\n--- Checking EventDate Format ---");
        let date_check_query = "SELECT \"EventDate\", \"CounterID\" FROM hits LIMIT 10";
        println!("Checking EventDate values: {}", date_check_query);

        match context.sql(date_check_query).await {
            Ok(df) => {
                match df.collect().await {
                    Ok(batches) => {
                        if !batches.is_empty() {
                            let formatted = arrow::util::pretty::pretty_format_batches(&batches)
                                .map_err(|e| anyhow::anyhow!("Failed to format batches: {}", e))?;
                            println!("{}", formatted);
                        }
                    }
                    Err(e) => println!("❌ Error collecting date check: {}", e),
                }
            }
            Err(e) => println!("❌ Error executing date check: {}", e),
        }

        // Check available CounterID values and date ranges
        println!("\n--- Data Exploration ---");
        let explore_query = r#"
            SELECT
                COUNT(DISTINCT "CounterID") as unique_counters,
                MIN("EventDate") as min_date,
                MAX("EventDate") as max_date,
                COUNT(*) as total_rows
            FROM hits
            LIMIT 1
        "#;

        match context.sql(explore_query).await {
            Ok(df) => {
                match df.collect().await {
                    Ok(batches) => {
                        if !batches.is_empty() {
                            let formatted = arrow::util::pretty::pretty_format_batches(&batches)
                                .map_err(|e| anyhow::anyhow!("Failed to format batches: {}", e))?;
                            println!("Dataset info:\n{}", formatted);
                        }
                    }
                    Err(e) => println!("❌ Error collecting exploration: {}", e),
                }
            }
            Err(e) => println!("❌ Error executing exploration: {}", e),
        }

        // Check for CounterID = 62 specifically
        let counter_check = "SELECT COUNT(*) FROM hits WHERE \"CounterID\" = 62";
        match context.sql(counter_check).await {
            Ok(df) => {
                match df.collect().await {
                    Ok(batches) => {
                        if !batches.is_empty() {
                            let formatted = arrow::util::pretty::pretty_format_batches(&batches)
                                .map_err(|e| anyhow::anyhow!("Failed to format batches: {}", e))?;
                            println!("Rows with CounterID=62:\n{}", formatted);
                        }
                    }
                    Err(e) => println!("❌ Error checking CounterID: {}", e),
                }
            }
            Err(e) => println!("❌ Error executing CounterID check: {}", e),
        }

        // Demonstrate timestamp casting
        println!("\n--- Schema Casting Examples ---");
        let casting_query = r#"
            SELECT
                "EventDate",
                "EventTime",
                to_timestamp_seconds("EventTime") as converted_timestamp
            FROM hits
            LIMIT 5
        "#;

        println!("Demonstrating schema casting:");
        println!("{}", casting_query);

        match context.sql(casting_query).await {
            Ok(df) => {
                match df.collect().await {
                    Ok(batches) => {
                        if !batches.is_empty() {
                            let formatted = arrow::util::pretty::pretty_format_batches(&batches)
                                .map_err(|e| anyhow::anyhow!("Failed to format batches: {}", e))?;
                            println!("Schema casting results:\n{}", formatted);
                        }
                    }
                    Err(e) => println!("❌ Error collecting casting demo: {}", e),
                }
            }
            Err(e) => println!("❌ Error executing casting demo: {}", e),
        }

        // Calculate days since epoch for July 2013
        println!("\n--- EventDate Analysis ---");
        println!("EventDate 15889 represents days since epoch (Jan 1, 1970)");
        println!("15889 days ≈ July 2013");

        // Execute the original query with direct date comparison (days since epoch)
        // July 1, 2013 = 15889 days, July 31, 2013 = 15919 days since epoch
        println!("\n--- Executing Original PageViews Query ---");
        let query = r#"
            SELECT "Title", COUNT(*) AS PageViews
            FROM hits
            WHERE "CounterID" = 62
                AND "EventDate" >= 15889
                AND "EventDate" <= 15919
                AND "DontCountHits" = 0
                AND "IsRefresh" = 0
                AND "Title" <> ''
            GROUP BY "Title"
            ORDER BY PageViews DESC
            LIMIT 10
        "#;

        println!("Executing query:\n{}", query);

        match context.sql(query).await {
            Ok(df) => {
                match df.collect().await {
                    Ok(batches) => {
                        println!("\n📊 Query Results:");

                        if !batches.is_empty() {
                            // Use arrow's simple pretty print
                            let formatted = arrow::util::pretty::pretty_format_batches(&batches)
                                .map_err(|e| anyhow::anyhow!("Failed to format batches: {}", e))?;
                            println!("{}", formatted);
                        } else {
                            println!("No results found.");
                        }
                    }
                    Err(e) => println!("❌ Error collecting results: {}", e),
                }
            }
            Err(e) => println!("❌ Error executing query: {}", e),
        }

    } else {
        println!("Parquet file not found at: {}", parquet_path);
    }

    Ok(())
}

/// Test function to read parquet schema with hardcoded path
async fn test_parquet_schema() -> Result<()> {
    // Hardcode a test parquet file path - you can change this to your actual parquet file
    // let parquet_path = "/Users/pudyodu/dfpython/accounts.parquet";
    // let parquet_path = "/Users/pudyodu/dfpython/hits_data.parquet";
    let parquet_path = "/Users/pudyodu/clickbench_queries/original_data/hits.parquet";

    println!("Testing parquet schema reading for: {}", parquet_path);

    if std::path::Path::new(parquet_path).exists() {
        let context = SessionContext::new();

        // Read and print the parquet file schema
        let table_url = ListingTableUrl::parse(parquet_path)?;
        let parquet_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(parquet_format));

        let schema = listing_options
            .infer_schema(&context.state(), &table_url)
            .await?;

        println!("Parquet file schema for '{}': {}", parquet_path, schema);

        // Register the parquet table and execute a simple SQL query
        context.register_parquet("hits", parquet_path, ParquetReadOptions::default()).await?;

        // Execute a simple SQL query
        let sql = "select count(*) from hits where \"AdvEngineID\" != 0;";
        println!("\nExecuting SQL query: {}", sql);

        let df = context.sql(sql).await?;

        // Print the logical plan
        println!("\nLogical Plan:");
        println!("{}", df.logical_plan().display_indent());

        // Generate and print the Substrait plan
        println!("\nGenerating Substrait Plan...");
        let substrait_plan = to_substrait_plan(df.logical_plan(), &context.state())?;

        // Convert Substrait plan to JSON
        let substrait_json = serde_json::to_string_pretty(&substrait_plan)?;
        println!("Substrait Plan (JSON):");
        println!("{}", substrait_json);

        // Write Substrait plan to file
        let file_path = "df_substrait_plan.txt";
        std::fs::write(file_path, &substrait_json)?;
        println!("\nSubstrait plan written to file: {}", file_path);

        // context.execute_logical_plan(
        // df.logical_plan()).await?;

        // Execute and show results
        // let results = df.collect().await?;

        // println!("\nQuery results:");
        // for batch in results {
        //     println!("{}", batch);
        // }
    } else {
        println!("Parquet file not found at: {}", parquet_path);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting DataFusion JNI library test...");
    // test_parquet_schema().await?;
    read_parquet_schema().await?;
    Ok(())
}
