use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;

/// KVMap Processor - transforms arrays of label/value pair objects into key-value objects
#[derive(SerialProcessorInit)]
pub struct KVMapProcessor<'a> {
    path: jmespath::Expression<'a>,
    label_field: String,
    value_field: String,
    output_label: String,
}

#[derive(Parser)]
/// Transform label/value arrays into key-value objects (within-event)
#[command(
    version,
    arg_required_else_help(true),
    long_about = r#"
Transforms arrays of label/value pair objects into a flat key-value object.

EXAMPLE INPUT:
  {"items": [{"name": "color", "val": "blue"}, {"name": "size", "val": "large"}]}

COMMAND:
  kvmap items -k name -v val -o result

EXAMPLE OUTPUT:
  {"items": [{"name": "color", "val": "blue"}, {"name": "size", "val": "large"}], "result": {"color": "blue", "size": "large"}}

Also works with a single object (treated as a one-element array).
"#
)]
struct KVMapArgs {
    /// JMESPath query to extract the object array
    #[arg(required(true))]
    path: Vec<String>,

    /// Field name containing the key/label in each object
    #[arg(short = 'k', long, default_value = "label")]
    label_field: String,

    /// Field name containing the value in each object
    #[arg(short = 'v', long, default_value = "value")]
    value_field: String,

    /// Output label for the transformed object
    #[arg(short, long, default_value = "transformed")]
    output_label: String,
}

impl SerialProcessor for KVMapProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("transform label/value arrays into key-value objects (within-event)".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = KVMapArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        Ok(Self {
            path,
            label_field: args.label_field,
            value_field: args.value_field,
            output_label: args.output_label,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let result = self.path.search(&input)?;

        // Pass through if null or not an array
        if result.is_null() {
            return Ok(vec![input]);
        }

        // Handle both arrays and single objects (treat object as single-element array)
        let items: Vec<_> = if let Some(arr) = result.as_array() {
            arr.iter().collect()
        } else if result.as_object().is_some() {
            vec![&result]
        } else {
            return Ok(vec![input]); // Not an array or object, pass through
        };

        // Build the key-value map from array elements
        let mut output_map = serde_json::Map::new();

        for element in items.iter() {
            if let Some(obj) = element.as_object() {
                // Extract label and value fields
                let label = match obj.get(&self.label_field) {
                    Some(l) if l.is_string() => l.as_string().unwrap().to_string(),
                    _ => continue, // Skip if label field missing or not a string
                };

                let value = match obj.get(&self.value_field) {
                    Some(v) => serde_json::to_value(v)?,
                    None => continue, // Skip if value field missing
                };

                // Insert into output map (last wins for duplicates)
                output_map.insert(label, value);
            }
        }

        // Append transformed object to event
        if let Some(event) = input.borrow_mut().as_object_mut() {
            event.insert(self.output_label.clone(), serde_json::Value::Object(output_map));
        }

        Ok(vec![input])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn create_event(json: serde_json::Value) -> Event {
        Rc::new(RefCell::new(json))
    }

    // Property 1: Core Transformation Correctness
    // For any event containing an array of objects at the configured JMESPath,
    // where each object has the configured label and value fields, the transformed
    // output object SHALL contain a key for each array element's label field value,
    // with the corresponding value field value as its value.
    // **Validates: Requirements 1.1, 3.1, 3.2, 4.1**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_core_transformation_correctness(
            labels in prop::collection::vec("[a-z]{1,10}", 1..5),
            values in prop::collection::vec(0.0f64..1000.0f64, 1..5),
        ) {
            // Ensure we have matching pairs
            let len = labels.len().min(values.len());
            let labels = &labels[..len];
            let values = &values[..len];

            // Build input array
            let array: Vec<serde_json::Value> = labels.iter().zip(values.iter())
                .map(|(l, v)| {
                    serde_json::json!({
                        "policy": l,
                        "score": v
                    })
                })
                .collect();

            let input_json = serde_json::json!({
                "evals": array
            });

            let event = create_event(input_json);

            let mut processor = KVMapProcessor::new(&[
                "kvmap".to_string(),
                "evals".to_string(),
                "-k".to_string(),
                "policy".to_string(),
                "-v".to_string(),
                "score".to_string(),
                "-o".to_string(),
                "scores".to_string(),
            ]).unwrap();

            let result = processor.process(event).unwrap();
            prop_assert_eq!(result.len(), 1);

            let output = result[0].borrow();
            let output_obj = output.as_object().unwrap();

            // Verify transformed object exists
            prop_assert!(output_obj.contains_key("scores"));

            let scores = output_obj.get("scores").unwrap().as_object().unwrap();

            // Build expected map (last wins for duplicates)
            let mut expected: std::collections::HashMap<&str, f64> = std::collections::HashMap::new();
            for (l, v) in labels.iter().zip(values.iter()) {
                expected.insert(l, *v);
            }

            // Verify each expected key-value pair
            for (label, value) in expected.iter() {
                prop_assert!(scores.contains_key(*label), "Missing key: {}", label);
                let actual = scores.get(*label).unwrap().as_f64().unwrap();
                prop_assert!((actual - value).abs() < f64::EPSILON, "Value mismatch for {}: expected {}, got {}", label, value, actual);
            }
        }

        // Property 2: Pass-Through for Invalid Extraction
        // For any event where the JMESPath query returns null, does not match, or returns
        // non-array data, the processor SHALL return the event unchanged (input equals output).
        // **Validates: Requirements 1.2, 1.3, 1.4**
        #[test]
        fn property_pass_through_for_invalid_extraction(
            field_name in "[a-z]{1,10}",
            field_value in "[a-z0-9]{1,20}",
        ) {
            // Test case 1: JMESPath path doesn't exist (returns null)
            let input_json = serde_json::json!({
                field_name.clone(): field_value.clone()
            });
            let original_json = input_json.clone();
            let event = create_event(input_json);

            let mut processor = KVMapProcessor::new(&[
                "kvmap".to_string(),
                "nonexistent_path".to_string(),
                "-o".to_string(),
                "output".to_string(),
            ]).unwrap();

            let result = processor.process(event).unwrap();
            prop_assert_eq!(result.len(), 1);

            let output = result[0].borrow();
            // Event should be unchanged (no output field added)
            prop_assert_eq!(output.clone(), original_json);

            // Test case 2: JMESPath returns non-array (string)
            let input_json2 = serde_json::json!({
                "data": "not_an_array"
            });
            let original_json2 = input_json2.clone();
            let event2 = create_event(input_json2);

            let mut processor2 = KVMapProcessor::new(&[
                "kvmap".to_string(),
                "data".to_string(),
                "-o".to_string(),
                "output".to_string(),
            ]).unwrap();

            let result2 = processor2.process(event2).unwrap();
            prop_assert_eq!(result2.len(), 1);

            let output2 = result2[0].borrow();
            prop_assert_eq!(output2.clone(), original_json2);

            // Test case 3: JMESPath returns non-array (number)
            let input_json3 = serde_json::json!({
                "data": 42
            });
            let original_json3 = input_json3.clone();
            let event3 = create_event(input_json3);

            let mut processor3 = KVMapProcessor::new(&[
                "kvmap".to_string(),
                "data".to_string(),
                "-o".to_string(),
                "output".to_string(),
            ]).unwrap();

            let result3 = processor3.process(event3).unwrap();
            prop_assert_eq!(result3.len(), 1);

            let output3 = result3[0].borrow();
            prop_assert_eq!(output3.clone(), original_json3);

            // Test case 4: JMESPath returns object - should be processed as single-element array
            let input_json4 = serde_json::json!({
                "data": {"label": "single_key", "value": "single_value"}
            });
            let event4 = create_event(input_json4);

            let mut processor4 = KVMapProcessor::new(&[
                "kvmap".to_string(),
                "data".to_string(),
                "-o".to_string(),
                "output".to_string(),
            ]).unwrap();

            let result4 = processor4.process(event4).unwrap();
            prop_assert_eq!(result4.len(), 1);

            let output4 = result4[0].borrow();
            let output_obj4 = output4.as_object().unwrap();
            // Object should be processed as single-element array, so output field should exist
            prop_assert!(output_obj4.contains_key("output"));
            let result_map4 = output_obj4.get("output").unwrap().as_object().unwrap();
            prop_assert_eq!(result_map4.len(), 1);
            prop_assert!(result_map4.contains_key("single_key"));
            prop_assert_eq!(result_map4.get("single_key").unwrap().as_str().unwrap(), "single_value");
        }

        // Property 3: Malformed Element Skipping
        // For any array of objects where some elements are missing the configured label field
        // or value field, the transformed output SHALL only contain entries for elements that
        // have both fields present.
        // **Validates: Requirements 3.3, 3.4**
        #[test]
        fn property_malformed_element_skipping(
            valid_labels in prop::collection::vec("[a-z]{1,10}", 1..5),
            valid_values in prop::collection::vec(0.0f64..1000.0f64, 1..5),
            num_missing_label in 0usize..3,
            num_missing_value in 0usize..3,
        ) {
            // Ensure we have matching pairs for valid elements
            let len = valid_labels.len().min(valid_values.len());
            let valid_labels = &valid_labels[..len];
            let valid_values = &valid_values[..len];

            // Build input array with valid elements
            let mut array: Vec<serde_json::Value> = valid_labels.iter().zip(valid_values.iter())
                .map(|(l, v)| {
                    serde_json::json!({
                        "label": l,
                        "value": v
                    })
                })
                .collect();

            // Add elements missing label field
            for i in 0..num_missing_label {
                array.push(serde_json::json!({
                    "value": i as f64 * 100.0
                }));
            }

            // Add elements missing value field
            for i in 0..num_missing_value {
                array.push(serde_json::json!({
                    "label": format!("missing_value_{}", i)
                }));
            }

            let input_json = serde_json::json!({
                "items": array
            });

            let event = create_event(input_json);

            let mut processor = KVMapProcessor::new(&[
                "kvmap".to_string(),
                "items".to_string(),
                "-o".to_string(),
                "result".to_string(),
            ]).unwrap();

            let result = processor.process(event).unwrap();
            prop_assert_eq!(result.len(), 1);

            let output = result[0].borrow();
            let output_obj = output.as_object().unwrap();
            let result_map = output_obj.get("result").unwrap().as_object().unwrap();

            // Calculate expected unique labels (duplicates are deduplicated, last wins)
            let unique_labels: std::collections::HashSet<&str> = valid_labels.iter().map(|s| s.as_str()).collect();

            // Result should only contain unique valid labels (those with both label and value)
            prop_assert_eq!(result_map.len(), unique_labels.len(),
                "Expected {} unique labels, got {} in result map. Labels: {:?}",
                unique_labels.len(), result_map.len(), valid_labels);

            // Verify all unique valid labels are present
            for label in unique_labels.iter() {
                prop_assert!(result_map.contains_key(*label), "Missing valid key: {}", label);
            }

            // Verify malformed elements are not present
            for i in 0..num_missing_value {
                let missing_key = format!("missing_value_{}", i);
                prop_assert!(!result_map.contains_key(&missing_key), "Malformed element should be skipped: {}", missing_key);
            }
        }

        // Property 4: Value Type Preservation
        // For any array element with a value field of any JSON type (number, string, boolean,
        // object, array, null), the corresponding value in the transformed output SHALL have
        // the same type and value.
        // **Validates: Requirements 3.5**
        #[test]
        fn property_value_type_preservation(
            string_val in "[a-z]{1,20}",
            int_val in -1000i64..1000i64,
            float_val in -1000.0f64..1000.0f64,
            bool_val in proptest::bool::ANY,
        ) {
            // Build input array with different value types
            let array = vec![
                serde_json::json!({"label": "string_type", "value": string_val.clone()}),
                serde_json::json!({"label": "int_type", "value": int_val}),
                serde_json::json!({"label": "float_type", "value": float_val}),
                serde_json::json!({"label": "bool_type", "value": bool_val}),
                serde_json::json!({"label": "null_type", "value": serde_json::Value::Null}),
                serde_json::json!({"label": "object_type", "value": {"nested": "object"}}),
                serde_json::json!({"label": "array_type", "value": [1, 2, 3]}),
            ];

            let input_json = serde_json::json!({
                "items": array
            });

            let event = create_event(input_json);

            let mut processor = KVMapProcessor::new(&[
                "kvmap".to_string(),
                "items".to_string(),
                "-o".to_string(),
                "result".to_string(),
            ]).unwrap();

            let result = processor.process(event).unwrap();
            prop_assert_eq!(result.len(), 1);

            let output = result[0].borrow();
            let output_obj = output.as_object().unwrap();
            let result_map = output_obj.get("result").unwrap().as_object().unwrap();

            // Verify string type preserved
            let string_result = result_map.get("string_type").unwrap();
            prop_assert!(string_result.is_string());
            prop_assert_eq!(string_result.as_str().unwrap(), string_val);

            // Verify integer type preserved (JSON numbers)
            let int_result = result_map.get("int_type").unwrap();
            prop_assert!(int_result.is_number());
            prop_assert_eq!(int_result.as_i64().unwrap(), int_val);

            // Verify float type preserved
            let float_result = result_map.get("float_type").unwrap();
            prop_assert!(float_result.is_number());
            prop_assert!((float_result.as_f64().unwrap() - float_val).abs() < f64::EPSILON);

            // Verify boolean type preserved
            let bool_result = result_map.get("bool_type").unwrap();
            prop_assert!(bool_result.is_boolean());
            prop_assert_eq!(bool_result.as_bool().unwrap(), bool_val);

            // Verify null type preserved
            let null_result = result_map.get("null_type").unwrap();
            prop_assert!(null_result.is_null());

            // Verify object type preserved
            let object_result = result_map.get("object_type").unwrap();
            prop_assert!(object_result.is_object());
            prop_assert_eq!(object_result.get("nested").unwrap().as_str().unwrap(), "object");

            // Verify array type preserved
            let array_result = result_map.get("array_type").unwrap();
            prop_assert!(array_result.is_array());
            let arr = array_result.as_array().unwrap();
            prop_assert_eq!(arr.len(), 3);
            prop_assert_eq!(arr[0].as_i64().unwrap(), 1);
            prop_assert_eq!(arr[1].as_i64().unwrap(), 2);
            prop_assert_eq!(arr[2].as_i64().unwrap(), 3);
        }

        // Property 5: Duplicate Label Last-Wins
        // For any array containing multiple objects with the same label field value,
        // the transformed output SHALL contain the value from the last occurrence in the array.
        // **Validates: Requirements 3.6**
        #[test]
        fn property_duplicate_label_last_wins(
            label in "[a-z]{1,10}",
            values in prop::collection::vec(0.0f64..1000.0f64, 2..10),
        ) {
            // Build input array with duplicate labels
            let array: Vec<serde_json::Value> = values.iter()
                .map(|v| {
                    serde_json::json!({
                        "label": label.clone(),
                        "value": v
                    })
                })
                .collect();

            let input_json = serde_json::json!({
                "items": array
            });

            let event = create_event(input_json);

            let mut processor = KVMapProcessor::new(&[
                "kvmap".to_string(),
                "items".to_string(),
                "-o".to_string(),
                "result".to_string(),
            ]).unwrap();

            let result = processor.process(event).unwrap();
            prop_assert_eq!(result.len(), 1);

            let output = result[0].borrow();
            let output_obj = output.as_object().unwrap();
            let result_map = output_obj.get("result").unwrap().as_object().unwrap();

            // Should only have one key (the duplicate label)
            prop_assert_eq!(result_map.len(), 1);

            // The value should be from the last occurrence
            let last_value = values.last().unwrap();
            let actual = result_map.get(&label).unwrap().as_f64().unwrap();
            prop_assert!((actual - last_value).abs() < f64::EPSILON,
                "Expected last value {}, got {}", last_value, actual);
        }

        // Property 6: Event Field Preservation
        // For any event processed by the transformer, all fields in the original event
        // (except the output label) SHALL remain unchanged in the output event.
        // **Validates: Requirements 4.2, 4.3**
        #[test]
        fn property_event_field_preservation(
            field1_name in "[a-z]{1,10}",
            field1_value in "[a-z0-9]{1,20}",
            field2_name in "[a-z]{1,10}",
            field2_value in -1000i64..1000i64,
            field3_name in "[a-z]{1,10}",
            field3_value in proptest::bool::ANY,
            labels in prop::collection::vec("[a-z]{1,10}", 1..3),
            values in prop::collection::vec(0.0f64..1000.0f64, 1..3),
        ) {
            // Ensure field names are distinct and don't conflict with reserved names
            let field1_name = format!("f1_{}", field1_name);
            let field2_name = format!("f2_{}", field2_name);
            let field3_name = format!("f3_{}", field3_name);
            let array_field = "items".to_string();
            let output_label = "result".to_string();

            // Ensure we have matching pairs
            let len = labels.len().min(values.len());
            let labels = &labels[..len];
            let values = &values[..len];

            // Build input array
            let array: Vec<serde_json::Value> = labels.iter().zip(values.iter())
                .map(|(l, v)| {
                    serde_json::json!({
                        "label": l,
                        "value": v
                    })
                })
                .collect();

            let input_json = serde_json::json!({
                field1_name.clone(): field1_value.clone(),
                field2_name.clone(): field2_value,
                field3_name.clone(): field3_value,
                array_field.clone(): array
            });

            let event = create_event(input_json);

            let mut processor = KVMapProcessor::new(&[
                "kvmap".to_string(),
                array_field.clone(),
                "-o".to_string(),
                output_label.clone(),
            ]).unwrap();

            let result = processor.process(event).unwrap();
            prop_assert_eq!(result.len(), 1);

            let output = result[0].borrow();
            let output_obj = output.as_object().unwrap();

            // Verify all original fields are preserved
            prop_assert!(output_obj.contains_key(&field1_name), "Field {} should be preserved", field1_name);
            prop_assert_eq!(output_obj.get(&field1_name).unwrap().as_str().unwrap(), field1_value);

            prop_assert!(output_obj.contains_key(&field2_name), "Field {} should be preserved", field2_name);
            prop_assert_eq!(output_obj.get(&field2_name).unwrap().as_i64().unwrap(), field2_value);

            prop_assert!(output_obj.contains_key(&field3_name), "Field {} should be preserved", field3_name);
            prop_assert_eq!(output_obj.get(&field3_name).unwrap().as_bool().unwrap(), field3_value);

            // Verify the source array field is preserved
            prop_assert!(output_obj.contains_key(&array_field), "Array field should be preserved");
            let preserved_array = output_obj.get(&array_field).unwrap().as_array().unwrap();
            prop_assert_eq!(preserved_array.len(), len);

            // Verify the output label was added
            prop_assert!(output_obj.contains_key(&output_label), "Output label should be added");

            // Verify the output contains the expected number of fields
            // (original fields + array field + output label)
            prop_assert_eq!(output_obj.len(), 5); // field1, field2, field3, items, result
        }
    }

    // Unit test: Single object treated as single-element array
    #[test]
    fn test_single_object_as_array() {
        // When JMESPath returns a single object (not an array), it should be
        // processed as if it were an array with one element
        let input_json = serde_json::json!({
            "single_item": {
                "label": "my_key",
                "value": 42
            }
        });

        let event = create_event(input_json);

        let mut processor = KVMapProcessor::new(&[
            "kvmap".to_string(),
            "single_item".to_string(),
            "-o".to_string(),
            "result".to_string(),
        ])
        .unwrap();

        let result = processor.process(event).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let output_obj = output.as_object().unwrap();

        // Verify the result field was created
        assert!(output_obj.contains_key("result"));
        let result_map = output_obj.get("result").unwrap().as_object().unwrap();

        // Verify the single object was processed correctly
        assert_eq!(result_map.len(), 1);
        assert!(result_map.contains_key("my_key"));
        assert_eq!(result_map.get("my_key").unwrap().as_i64().unwrap(), 42);

        // Verify original field is preserved
        assert!(output_obj.contains_key("single_item"));
    }

    // Unit test: Single object with custom field names
    #[test]
    fn test_single_object_custom_fields() {
        let input_json = serde_json::json!({
            "eval": {
                "policy": "high/content",
                "score": 0.9876
            }
        });

        let event = create_event(input_json);

        let mut processor = KVMapProcessor::new(&[
            "kvmap".to_string(),
            "eval".to_string(),
            "-k".to_string(),
            "policy".to_string(),
            "-v".to_string(),
            "score".to_string(),
            "-o".to_string(),
            "scores".to_string(),
        ])
        .unwrap();

        let result = processor.process(event).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let output_obj = output.as_object().unwrap();

        let scores = output_obj.get("scores").unwrap().as_object().unwrap();
        assert_eq!(scores.len(), 1);
        assert!(scores.contains_key("high/content"));
        assert!(
            (scores.get("high/content").unwrap().as_f64().unwrap() - 0.9876).abs() < f64::EPSILON
        );
    }

    // Unit test: Single object missing required fields should produce empty result
    #[test]
    fn test_single_object_missing_fields() {
        let input_json = serde_json::json!({
            "item": {
                "other_field": "some_value"
            }
        });

        let event = create_event(input_json);

        let mut processor = KVMapProcessor::new(&[
            "kvmap".to_string(),
            "item".to_string(),
            "-o".to_string(),
            "result".to_string(),
        ])
        .unwrap();

        let result = processor.process(event).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let output_obj = output.as_object().unwrap();

        // Result should exist but be empty (object was processed but had no valid label/value)
        assert!(output_obj.contains_key("result"));
        let result_map = output_obj.get("result").unwrap().as_object().unwrap();
        assert_eq!(result_map.len(), 0);
    }
}
