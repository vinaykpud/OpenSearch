/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::error::Error;

use jni::objects::JObject;
use jni::sys::jlong;
use jni::JNIEnv;

/// Set error message from a result using a Consumer<String> Java callback
pub fn set_error_message<Err: Error>(env: &mut JNIEnv, callback: JObject, result: Result<(), Err>) {
    match result {
        Ok(_) => {
            let err_message = JObject::null();
            env.call_method(
                callback,
                "accept",
                "(Ljava/lang/Object;)V",
                &[(&err_message).into()],
            )
                .expect("Failed to call error handler with null message");
        }
        Err(err) => {
            let err_message = env
                .new_string(err.to_string())
                .expect("Couldn't create java string for error message");
            env.call_method(
                callback,
                "accept",
                "(Ljava/lang/Object;)V",
                &[(&err_message).into()],
            )
                .expect("Failed to call error handler with error message");
        }
    };
}

/// Call an ObjectResultCallback to return either a pointer to a newly created object or an error message
pub fn set_object_result<T, Err: Error>(
    env: &mut JNIEnv,
    callback: JObject,
    address: Result<*mut T, Err>,
) {
    match address {
        Ok(address) => set_object_result_ok(env, callback, address),
        Err(err) => set_object_result_error(env, callback, &err),
    };
}

/// Set success result by calling an ObjectResultCallback
// pub fn set_object_result_ok<T>(env: &mut JNIEnv, callback: JObject, address: *mut T) {
//     let err_message = JObject::null();
//     env.call_method(
//         callback,
//         "callback",
//         "(Ljava/lang/String;J)V",
//         &[(&err_message).into(), (address as jlong).into()],
//     )
//         .expect("Failed to call object result callback with address");
// }

pub fn set_object_result_ok<T>(env: &mut JNIEnv, callback: JObject, address: *mut T) {
    let err_message = env
        .new_string("")
        .expect("Couldn't create empty java string");

    println!("About to call Java callback...");

    let result = env.call_method(
        callback,
        "callback",
        "(Ljava/lang/String;J)V",
        &[(&err_message).into(), (address as jlong).into()],
    );

    match result {
        Ok(_) => {
            println!("Java callback completed successfully - no Rust cleanup issue");
        }
        Err(jni_error) => {
            println!("Java callback failed with JNI error: {:?}", jni_error);

            // Check what kind of Java exception occurred
            if let Ok(true) = env.exception_check() {
                println!("There IS a pending Java exception:");
                let _ = env.exception_describe(); // This prints the Java stack trace
                let _ = env.exception_clear();
            } else {
                println!("No Java exception - this would be a pure JNI issue");
            }
            // Don't panic
            return;
        }
    }
}

/// Set error result by calling an ObjectResultCallback
pub fn set_object_result_error<T: Error>(env: &mut JNIEnv, callback: JObject, error: &T) {
    let err_message = env
        .new_string(error.to_string())
        .expect("Couldn't create java string for error message");
    let address = -1 as jlong;
    env.call_method(
        callback,
        "callback",
        "(Ljava/lang/String;J)V",
        &[(&err_message).into(), address.into()],
    )
        .expect("Failed to call object result callback with error");
}
