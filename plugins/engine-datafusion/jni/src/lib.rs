/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
mod util;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::physical_plan::SendableRecordBatchStream;
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use datafusion::DATAFUSION_VERSION;

use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use prost::Message;

use crate::util::{set_object_result_error, set_object_result_ok};
use anyhow::Result;
use arrow::array::{Array, StructArray};
use futures::stream::StreamExt;
use futures::TryStreamExt;
use std::ptr::addr_of_mut;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_core_SessionContext_createContext(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config(config);
    let ctx = Box::into_raw(Box::new(context)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_core_SessionContext_createRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    if let Ok(runtime) = Runtime::new() {
        Box::into_raw(Box::new(runtime)) as jlong
    } else {
        // TODO error handling
        -1
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_core_SessionContext_registerParquetTable(
    mut env: JNIEnv,
    _class: JClass,
    context_id: jlong,
    runtime_id: jlong,
    parquet_file_path: JString,
    table_name: JString
) -> jlong {
    if context_id == 0 {
        let _ = env.throw_new("java/lang/RuntimeException", "Invalid context ID");
        return 0;
    }

    if runtime_id == 0 {
        let _ = env.throw_new("java/lang/RuntimeException", "Invalid runtime ID");
        return 0;
    }

    let parquet_path: String = match env.get_string(&parquet_file_path) {
        Ok(path) => path.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException",
                                  &format!("Failed to get parquet file path: {}", e));
            return 0;
        }
    };

    let table_name_str: String = match env.get_string(&table_name) {
        Ok(name) => name.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException",
                                  &format!("Failed to get table name: {}", e));
            return 0;
        }
    };

    let context = unsafe { &*(context_id as *const SessionContext) };
    let runtime = unsafe { &*(runtime_id as *const Runtime) };

    match runtime.block_on(async {
        if std::path::Path::new(&parquet_path).exists() {
            context.register_parquet(&table_name_str, &parquet_path, ParquetReadOptions::default()).await
        } else {
            Err(datafusion::error::DataFusionError::Execution(
                format!("Parquet file not found: {}", parquet_path)
            ))
        }
    }) {
        Ok(_) => 1, // Success
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException",
                                  &format!("Failed to register parquet table: {}", e));
            0 // Failure
        }
    }
}

/// Close and cleanup a DataFusion context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_core_SessionContext_closeContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    if context_id != 0 {
        let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_core_SessionContext_closeRuntime(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    if pointer != 0 {
        let _ = unsafe { Box::from_raw(pointer as *mut Runtime) };
    }
}

/// Execute a Substrait query plan and return SendableRecordBatchStream as jlong
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionService_nativeExecuteSubstraitQueryStream(
    env: JNIEnv,
    _class: JClass,
    runtime_id: jlong,
    context_id: jlong,
    query_plan_bytes: jbyteArray,
) -> jlong {
    println!("DataFusionService_nativeExecuteSubstraitQueryStream: Starting execution");
    println!("runtime_id: {}, context_id: {}", runtime_id, context_id);

    let runtime = unsafe { &*(runtime_id as *const Runtime) };
    let context = unsafe { &*(context_id as *const SessionContext) };
    println!("Retrieved runtime and context pointers successfully");

    println!("query_plan_bytes raw pointer: {:?}", query_plan_bytes);

    if query_plan_bytes.is_null() {
        println!("ERROR: query_plan_bytes is null!");
        return 0;
    }

    let byte_array = unsafe { JByteArray::from_raw(query_plan_bytes) };
    println!("Created JByteArray from raw pointer");

    let plan_bytes = match env.convert_byte_array(byte_array) {
        Ok(bytes) => {
            println!("Successfully converted byte array, size: {} bytes", bytes.len());
            bytes
        },
        Err(e) => {
            println!("Failed to convert byte array: {:?}", e);
            return 0; // Return 0 on error
        }
    };

    println!("Starting async block execution");
    runtime.block_on(async {
        println!("Decoding Substrait plan...");
        let substrait_plan = datafusion_substrait::substrait::proto::Plan::decode(&plan_bytes[..]).unwrap();
        println!("Substrait plan decoded successfully, relations: {}", substrait_plan.relations.len());

        println!("Converting Substrait plan to DataFusion logical plan...");
        let logical_plan = from_substrait_plan(&context.state(), &substrait_plan).await.unwrap();
        println!("Logical plan created successfully");

        println!("Executing logical plan...");
        let dataframe = context.execute_logical_plan(logical_plan).await.unwrap();
        println!("DataFrame created successfully");

        println!("Getting execution stream...");
        let stream = dataframe.execute_stream().await.unwrap();
        println!("Stream created successfully");

        let stream_ptr = Box::into_raw(Box::new(stream)) as jlong;
        println!("Stream pointer created: {}", stream_ptr);
        stream_ptr
    })
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_next(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    stream: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    runtime.block_on(async {
        let next = stream.try_next().await;
        match next {
            Ok(Some(batch)) => {
                // Convert to struct array for compatibility with FFI
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let mut ffi_array = FFI_ArrowArray::new(&array_data);
                // ffi_array must remain alive until after the callback is called
                set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_array));
            }
            Ok(None) => {
                set_object_result_ok(&mut env, callback, 0 as *mut FFI_ArrowSchema);
            }
            Err(err) => {
                set_object_result_error(&mut env, callback, &err);
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_getSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream: jlong,
    callback: JObject,
) {
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    let schema = stream.schema();
    // Print field details for debugging
    for (i, field) in schema.fields().iter().enumerate() {
        println!("  Field {}: name='{}', type={:?}, nullable={}",
                 i, field.name(), field.data_type(), field.is_nullable());
    }
    let ffi_schema = FFI_ArrowSchema::try_from(&*schema);
    match ffi_schema {
        Ok(mut ffi_schema) => {
            println!("Created FFI schema successfully, about to call Java...");
            // ffi_schema must remain alive until after the callback is called
            set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_schema));
            println!("Returned from Java callback");
        }
        Err(err) => {
            set_object_result_error(&mut env, callback, &err);
        }
    }
    println!("Rust function ending normally");
}



#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_closeStream(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    if pointer != 0 {
        let _ = unsafe { Box::from_raw(pointer as *mut SendableRecordBatchStream) };
    }
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

////////////////////////////////////////////////////////////////// Simple function to read and print parquet schema
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
