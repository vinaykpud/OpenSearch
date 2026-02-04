/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! DataFusion JNI bridge for OpenSearch query execution.
//!
//! This library provides JNI bindings that allow Java code to build and execute
//! DataFusion queries incrementally. Each operation (filter, project, aggregate, etc.)
//! adds to a lazy DataFrame. Execution only happens when collect() is called,
//! allowing DataFusion to optimize the entire plan.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::prelude::*;
use datafusion::logical_expr::SortExpr;
use datafusion::functions_aggregate::expr_fn::{sum, count, avg, min, max};
use jni::objects::{JBooleanArray, JClass, JObjectArray, JString};
use jni::sys::{jbyteArray, jint, jlong};
use jni::JNIEnv;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Thread-local Tokio runtime for async DataFusion operations.
/// Each thread gets its own runtime to avoid contention.
thread_local! {
    static RUNTIME: Runtime = Runtime::new().expect("Failed to create Tokio runtime");
}

/// Wrapper to hold SessionContext on the heap.
struct ContextWrapper {
    ctx: SessionContext,
}

/// Wrapper to hold DataFrame on the heap.
struct DataFrameWrapper {
    df: DataFrame,
}

// =============================================================================
// Context Management
// =============================================================================

/// Create a new DataFusion SessionContext.
/// Returns a handle (pointer) to the context.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_queryplanner_engine_datafusion_DataFusionBridge_createContext<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    let ctx = SessionContext::new();
    let wrapper = Box::new(ContextWrapper { ctx });
    Box::into_raw(wrapper) as jlong
}

/// Free a SessionContext.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_queryplanner_engine_datafusion_DataFusionBridge_freeContext<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    ctx_ptr: jlong,
) {
    if ctx_ptr != 0 {
        unsafe {
            let _ = Box::from_raw(ctx_ptr as *mut ContextWrapper);
        }
    }
}

// =============================================================================
// Data Sources
// =============================================================================

/// Scan a table by name and return a lazy DataFrame handle.
///
/// For testing, this creates a hardcoded in-memory table with sample data.
/// In the future, this will look up the data source based on table name.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_queryplanner_engine_datafusion_DataFusionBridge_scan<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    ctx_ptr: jlong,
    table_name: JString<'local>,
) -> jlong {
    let result = (|| -> Result<jlong, String> {
        let ctx_wrapper = unsafe { &*(ctx_ptr as *const ContextWrapper) };
        let table_name_str: String = env
            .get_string(&table_name)
            .map_err(|e| format!("Failed to get table name: {}", e))?
            .into();

        // Create hardcoded test data matching our orders schema
        let batch = create_test_orders_batch()
            .map_err(|e| format!("Failed to create test batch: {}", e))?;

        RUNTIME.with(|rt| {
            rt.block_on(async {
                // Create a memory table from the batch
                let schema = batch.schema();
                let provider = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
                    .map_err(|e| format!("Failed to create MemTable: {}", e))?;

                // Register and query the table
                ctx_wrapper
                    .ctx
                    .register_table("_scan_data", Arc::new(provider))
                    .map_err(|e| format!("Failed to register table: {}", e))?;

                let df = ctx_wrapper
                    .ctx
                    .table("_scan_data")
                    .await
                    .map_err(|e| format!("Failed to get table: {}", e))?;

                let wrapper = Box::new(DataFrameWrapper { df });
                Ok(Box::into_raw(wrapper) as jlong)
            })
        })
    })();

    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e);
            0
        }
    }
}

/// Create a hardcoded test batch with orders data.
fn create_test_orders_batch() -> Result<RecordBatch, datafusion::arrow::error::ArrowError> {
    use datafusion::arrow::array::{Float64Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("quantity", DataType::Int32, false),
    ]));

    let order_id = StringArray::from(vec![
        "ORD-001", "ORD-002", "ORD-003", "ORD-004", "ORD-005",
        "ORD-006", "ORD-007", "ORD-008", "ORD-009", "ORD-010",
    ]);
    let customer_id = StringArray::from(vec![
        "CUST-A", "CUST-B", "CUST-A", "CUST-C", "CUST-B",
        "CUST-A", "CUST-D", "CUST-C", "CUST-B", "CUST-A",
    ]);
    let status = StringArray::from(vec![
        "active", "active", "completed", "active", "cancelled",
        "active", "active", "completed", "active", "active",
    ]);
    let category = StringArray::from(vec![
        "electronics", "clothing", "electronics", "books", "clothing",
        "books", "electronics", "clothing", "electronics", "books",
    ]);
    let amount = Float64Array::from(vec![
        150.0, 80.0, 220.0, 30.0, 110.0, 45.0, 350.0, 65.0, 180.0, 25.0,
    ]);
    let quantity = Int32Array::from(vec![2, 3, 1, 5, 2, 3, 1, 4, 2, 2]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(order_id),
            Arc::new(customer_id),
            Arc::new(status),
            Arc::new(category),
            Arc::new(amount),
            Arc::new(quantity),
        ],
    )
}

// =============================================================================
// DataFrame Operations (all lazy)
// =============================================================================

/// Apply a filter to the DataFrame.
/// The filter expression is a SQL-style expression string (e.g., "price > 100").
#[no_mangle]
pub extern "system" fn Java_org_opensearch_queryplanner_engine_datafusion_DataFusionBridge_filter<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    df_ptr: jlong,
    filter_expr: JString<'local>,
) -> jlong {
    let result = (|| -> Result<jlong, String> {
        let df_wrapper = unsafe { &*(df_ptr as *const DataFrameWrapper) };
        let expr_str: String = env
            .get_string(&filter_expr)
            .map_err(|e| format!("Failed to get filter expression: {}", e))?
            .into();

        RUNTIME.with(|rt| {
            rt.block_on(async {
                // Parse the SQL expression
                let expr = df_wrapper
                    .df
                    .parse_sql_expr(&expr_str)
                    .map_err(|e| format!("Failed to parse filter expression '{}': {}", expr_str, e))?;

                let new_df = df_wrapper
                    .df
                    .clone()
                    .filter(expr)
                    .map_err(|e| format!("Failed to apply filter: {}", e))?;

                let wrapper = Box::new(DataFrameWrapper { df: new_df });
                Ok(Box::into_raw(wrapper) as jlong)
            })
        })
    })();

    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e);
            0
        }
    }
}

/// Project (select) columns from the DataFrame.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_queryplanner_engine_datafusion_DataFusionBridge_project<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    df_ptr: jlong,
    columns: JObjectArray<'local>,
) -> jlong {
    let result = (|| -> Result<jlong, String> {
        let df_wrapper = unsafe { &*(df_ptr as *const DataFrameWrapper) };
        let cols = java_string_array_to_vec(&mut env, &columns)?;

        RUNTIME.with(|rt| {
            rt.block_on(async {
                let exprs: Vec<Expr> = cols.iter().map(|c| col(c.as_str())).collect();

                let new_df = df_wrapper
                    .df
                    .clone()
                    .select(exprs)
                    .map_err(|e| format!("Failed to project columns: {}", e))?;

                let wrapper = Box::new(DataFrameWrapper { df: new_df });
                Ok(Box::into_raw(wrapper) as jlong)
            })
        })
    })();

    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e);
            0
        }
    }
}

/// Aggregate the DataFrame with GROUP BY.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_queryplanner_engine_datafusion_DataFusionBridge_aggregate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    df_ptr: jlong,
    group_by: JObjectArray<'local>,
    agg_funcs: JObjectArray<'local>,
    agg_cols: JObjectArray<'local>,
) -> jlong {
    let result = (|| -> Result<jlong, String> {
        let df_wrapper = unsafe { &*(df_ptr as *const DataFrameWrapper) };

        let group_cols = java_string_array_to_vec(&mut env, &group_by)?;
        let funcs = java_string_array_to_vec(&mut env, &agg_funcs)?;
        let cols = java_string_array_to_vec(&mut env, &agg_cols)?;

        RUNTIME.with(|rt| {
            rt.block_on(async {
                let group_exprs: Vec<Expr> = group_cols.iter().map(|c| col(c.as_str())).collect();

                let agg_exprs: Vec<Expr> = funcs
                    .iter()
                    .zip(cols.iter())
                    .map(|(func, c)| build_agg_expr(func, c))
                    .collect::<Result<Vec<_>, _>>()?;

                let new_df = df_wrapper
                    .df
                    .clone()
                    .aggregate(group_exprs, agg_exprs)
                    .map_err(|e| format!("Failed to aggregate: {}", e))?;

                let wrapper = Box::new(DataFrameWrapper { df: new_df });
                Ok(Box::into_raw(wrapper) as jlong)
            })
        })
    })();

    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e);
            0
        }
    }
}

/// Sort the DataFrame.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_queryplanner_engine_datafusion_DataFusionBridge_sort<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    df_ptr: jlong,
    columns: JObjectArray<'local>,
    ascending: JBooleanArray<'local>,
) -> jlong {
    let result = (|| -> Result<jlong, String> {
        let df_wrapper = unsafe { &*(df_ptr as *const DataFrameWrapper) };

        let cols = java_string_array_to_vec(&mut env, &columns)?;
        let asc = java_boolean_array_to_vec(&mut env, &ascending)?;

        RUNTIME.with(|rt| {
            rt.block_on(async {
                // Build SortExpr for each column
                let sort_exprs: Vec<SortExpr> = cols
                    .iter()
                    .zip(asc.iter())
                    .map(|(c, &asc)| {
                        SortExpr {
                            expr: col(c.as_str()),
                            asc,
                            nulls_first: true,
                        }
                    })
                    .collect();

                let new_df = df_wrapper
                    .df
                    .clone()
                    .sort(sort_exprs)
                    .map_err(|e| format!("Failed to sort: {}", e))?;

                let wrapper = Box::new(DataFrameWrapper { df: new_df });
                Ok(Box::into_raw(wrapper) as jlong)
            })
        })
    })();

    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e);
            0
        }
    }
}

/// Limit the number of rows in the DataFrame.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_queryplanner_engine_datafusion_DataFusionBridge_limit<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    df_ptr: jlong,
    count: jint,
) -> jlong {
    let result = (|| -> Result<jlong, String> {
        let df_wrapper = unsafe { &*(df_ptr as *const DataFrameWrapper) };

        RUNTIME.with(|rt| {
            rt.block_on(async {
                let new_df = df_wrapper
                    .df
                    .clone()
                    .limit(0, Some(count as usize))
                    .map_err(|e| format!("Failed to limit: {}", e))?;

                let wrapper = Box::new(DataFrameWrapper { df: new_df });
                Ok(Box::into_raw(wrapper) as jlong)
            })
        })
    })();

    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e);
            0
        }
    }
}

// =============================================================================
// Execution
// =============================================================================

/// Execute the DataFrame plan and collect results as Arrow IPC bytes.
/// This is where DataFusion optimizes the entire plan and executes it.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_queryplanner_engine_datafusion_DataFusionBridge_collect<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    df_ptr: jlong,
) -> jbyteArray {
    let result = (|| -> Result<Vec<u8>, String> {
        let df_wrapper = unsafe { &*(df_ptr as *const DataFrameWrapper) };

        RUNTIME.with(|rt| {
            rt.block_on(async {
                // This is where the magic happens - DataFusion optimizes and executes
                let batches = df_wrapper
                    .df
                    .clone()
                    .collect()
                    .await
                    .map_err(|e| format!("Failed to collect results: {}", e))?;

                // Serialize to Arrow IPC format
                serialize_batches_to_ipc(&batches)
            })
        })
    })();

    match result {
        Ok(ipc_bytes) => {
            let output = env
                .new_byte_array(ipc_bytes.len() as i32)
                .expect("Failed to create byte array");

            // Convert Vec<u8> to &[i8] for JNI
            let signed_bytes: Vec<i8> = ipc_bytes.into_iter().map(|b| b as i8).collect();
            env.set_byte_array_region(&output, 0, &signed_bytes)
                .expect("Failed to set byte array region");

            output.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e);
            std::ptr::null_mut()
        }
    }
}

/// Free a DataFrame handle.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_queryplanner_engine_datafusion_DataFusionBridge_freeDataFrame<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    df_ptr: jlong,
) {
    if df_ptr != 0 {
        unsafe {
            let _ = Box::from_raw(df_ptr as *mut DataFrameWrapper);
        }
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Convert a Java String array to a Rust Vec<String>.
fn java_string_array_to_vec(env: &mut JNIEnv, array: &JObjectArray) -> Result<Vec<String>, String> {
    let len = env
        .get_array_length(array)
        .map_err(|e| format!("Failed to get array length: {}", e))?;

    let mut result = Vec::with_capacity(len as usize);

    for i in 0..len {
        let obj = env
            .get_object_array_element(array, i)
            .map_err(|e| format!("Failed to get array element {}: {}", i, e))?;

        let jstr = JString::from(obj);
        let s: String = env
            .get_string(&jstr)
            .map_err(|e| format!("Failed to convert string at index {}: {}", i, e))?
            .into();

        result.push(s);
    }

    Ok(result)
}

/// Convert a Java boolean array to a Rust Vec<bool>.
fn java_boolean_array_to_vec(env: &mut JNIEnv, array: &JBooleanArray) -> Result<Vec<bool>, String> {
    let len = env
        .get_array_length(array)
        .map_err(|e| format!("Failed to get array length: {}", e))?;

    let mut buf = vec![0u8; len as usize];
    env.get_boolean_array_region(array, 0, &mut buf)
        .map_err(|e| format!("Failed to get boolean array region: {}", e))?;

    Ok(buf.into_iter().map(|b| b != 0).collect())
}

/// Build an aggregate expression from function name and column name.
fn build_agg_expr(func: &str, column: &str) -> Result<Expr, String> {
    let col_expr = if column == "*" {
        lit(1) // COUNT(*) uses a literal
    } else {
        col(column)
    };

    match func.to_uppercase().as_str() {
        "SUM" => Ok(sum(col_expr)),
        "COUNT" => Ok(count(col_expr)),
        "AVG" => Ok(avg(col_expr)),
        "MIN" => Ok(min(col_expr)),
        "MAX" => Ok(max(col_expr)),
        _ => Err(format!("Unknown aggregate function: {}", func)),
    }
}

/// Serialize RecordBatches to Arrow IPC format.
fn serialize_batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, String> {
    if batches.is_empty() {
        // Return empty IPC stream with no schema
        return Ok(Vec::new());
    }

    let schema = batches[0].schema();
    let mut buffer = Vec::new();

    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)
            .map_err(|e| format!("Failed to create IPC writer: {}", e))?;

        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| format!("Failed to write batch: {}", e))?;
        }

        writer
            .finish()
            .map_err(|e| format!("Failed to finish IPC stream: {}", e))?;
    }

    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_agg_expr() {
        assert!(build_agg_expr("SUM", "price").is_ok());
        assert!(build_agg_expr("COUNT", "*").is_ok());
        assert!(build_agg_expr("AVG", "quantity").is_ok());
        assert!(build_agg_expr("UNKNOWN", "col").is_err());
    }
}
