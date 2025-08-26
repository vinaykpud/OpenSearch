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

use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use prost::Message;

use anyhow::Result;
use std::sync::Arc;
use tokio::runtime::Runtime;

struct DataFusionContext {
    context: SessionContext,
    runtime: Arc<Runtime>,
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
            context.register_parquet("accounts", parquet_path, ParquetReadOptions::default()).await?;
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
