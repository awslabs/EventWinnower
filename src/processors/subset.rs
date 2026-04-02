use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

pub enum PathType {
    Node((String, Box<PathType>)),
    End,
}

impl PathType {
    pub fn new(path: &str) -> PathType {
        let vpath: Vec<&str> = path.split('.').collect();

        let mut working = PathType::End;
        for sub in vpath.iter().rev() {
            working = PathType::Node((sub.to_string(), Box::new(working)));
        }
        working
    }
    pub fn create_search(paths: Vec<String>) -> Vec<PathType> {
        let mut search: Vec<PathType> = Vec::new();
        for path in paths.iter() {
            search.push(PathType::new(path));
        }
        search
    }
    #[allow(dead_code)]
    pub fn is_end(&self) -> bool {
        matches!(self, PathType::End)
    }
    #[allow(dead_code)]
    pub fn find(&self, event: &serde_json::Value) -> Option<(String, serde_json::Value)> {
        let mut working_search = self;
        let mut working_haystack = event;

        while let PathType::Node((sub, next)) = working_search {
            if let Some(haystack_map) = working_haystack.as_object() {
                if let Some(element) = haystack_map.get(sub) {
                    if next.is_end() {
                        return Some((sub.clone(), element.clone()));
                    } else {
                        working_haystack = element;
                        working_search = next;
                    }
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
        None
    }

    #[allow(dead_code)]
    pub fn extract<'a>(
        &self,
        event: &'a mut serde_json::Value,
    ) -> Option<(serde_json::Value, &'a mut serde_json::Map<String, serde_json::Value>, String)>
    {
        let mut working_search = self;
        let mut working_haystack = event;

        while let PathType::Node((sub, next)) = working_search {
            if let Some(haystack_map) = working_haystack.as_object_mut() {
                if next.is_end() {
                    if let Some(element) = haystack_map.remove(sub) {
                        return Some((element, haystack_map, sub.clone()));
                    } else {
                        return None;
                    }
                } else if let Some(element) = haystack_map.get_mut(sub) {
                    working_haystack = element;
                    working_search = next;
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
        None
    }
}

#[derive(SerialProcessorInit)]
pub struct DenestProcessor {
    search: Vec<PathType>,
    into: Option<String>,
}

#[derive(Parser)]
/// extract out elements from a nested event using dotted path notation
#[command(version, long_about = None, arg_required_else_help(true))]
struct DenestProcessorArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long)]
    into: Option<String>,
}

impl SerialProcessor for DenestProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("flatten nested objects to top level".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = DenestProcessorArgs::try_parse_from(argv)?;
        let search = PathType::create_search(args.path);

        Ok(Self { search, into: args.into })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let mut output_map = serde_json::Map::new();

        {
            let event = input.borrow();
            if event.is_object() {
                for search in self.search.iter() {
                    if let Some((key, val)) = search.find(&event) {
                        output_map.insert(key, val);
                    }
                }
            } else {
                return Ok(vec![]);
            }
        } //end borrow

        if output_map.is_empty() {
            if self.into.is_none() {
                Ok(vec![])
            } else {
                Ok(vec![input])
            }
        } else {
            let output_value: serde_json::Value = output_map.into();

            if let Some(label) = &self.into {
                if let Some(output) = input.borrow_mut().as_object_mut() {
                    output.insert(label.clone(), output_value);
                } else {
                    return Ok(vec![]);
                }
                Ok(vec![input])
            } else {
                let output = Rc::new(RefCell::new(output_value));
                Ok(vec![output])
            }
        }
    }
}

#[derive(SerialProcessorInit)]
pub struct RemoveProcessor {
    search: HashMap<String, bool>,
}

#[derive(Parser)]
/// remove parent elements from an event
#[command(version, long_about = None, arg_required_else_help(true))]
struct RemoveProcessorArgs {
    #[arg(required(true))]
    path: Vec<String>,
}

impl SerialProcessor for RemoveProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("remove specified keys from events".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = RemoveProcessorArgs::try_parse_from(argv)?;
        let mut search: HashMap<String, bool> = HashMap::new();

        args.path.iter().for_each(|p| {
            search.insert(p.clone(), true);
        });

        Ok(Self { search })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let mut output_map = serde_json::Map::new();

        if !input.borrow().is_object() {
            return Ok(vec![input]);
        } else if let Some(event) = input.borrow().as_object() {
            for key in event.keys() {
                if !self.search.contains_key(key) {
                    output_map.insert(key.clone(), event[key].clone());
                }
            }
        }

        if output_map.is_empty() {
            Ok(vec![])
        } else {
            let output_value: serde_json::Value = output_map.into();

            let output = Rc::new(RefCell::new(output_value));
            Ok(vec![output])
        }
    }
}

#[derive(SerialProcessorInit)]
pub struct NestProcessor<'a> {
    path: Option<jmespath::Expression<'a>>,
    label: String,
}

#[derive(Parser)]
/// take entire event (or jmespath subset) and append it as a named object map
#[command(version, long_about = None, arg_required_else_help(false))]
struct NestProcessorArgs {
    path: Vec<String>,

    #[arg(short, long, default_value = "nest")]
    into: String,
}

impl SerialProcessor for NestProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("nest event fields under specified key".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = NestProcessorArgs::try_parse_from(argv)?;

        let path = if !args.path.is_empty() {
            Some(jmespath::compile(&args.path.join(" "))?)
        } else {
            None
        };

        Ok(Self { path, label: args.into })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let mut output_map = serde_json::Map::new();
        if let Some(path) = &self.path {
            let found = path.search(&input)?;
            if found.is_null() {
                return Ok(vec![]);
            }
            output_map.insert(self.label.clone(), serde_json::to_value(found)?);
        } else {
            output_map.insert(self.label.clone(), serde_json::to_value(input)?);
        }
        let output_value: serde_json::Value = output_map.into();
        Ok(vec![Rc::new(RefCell::new(output_value))])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // DenestProcessor Tests
    #[test]
    fn denest_test() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar things").unwrap();
        let mut subp = DenestProcessor::new(&args)?;
        let val = json!({
            "foo" : { "bar": "eval", },
            "things" : 123,
            "other" :"stuff"
        });

        let event = event_new(val);
        let mut output = subp.process(event.clone())?;
        assert_eq!(output.len(), 1);
        if let Some(output_event) = output.pop() {
            let output = output_event.borrow();
            assert_eq!(output["bar"], serde_json::to_value("eval").unwrap());
            assert_eq!(output["things"], serde_json::to_value(123).unwrap());
            assert!(output["foo"].is_null());
            assert!(output["other"].is_null());
        }

        let args = shlex::split("proc foo.bar things -i stuff").unwrap();
        let mut subp = DenestProcessor::new(&args)?;
        let mut output = subp.process(event.clone())?;

        assert_eq!(output.len(), 1);
        if let Some(output_event) = output.pop() {
            let output = output_event.borrow();
            assert_eq!(output["other"], serde_json::to_value("stuff").unwrap());
            assert!(output["stuff"].is_object());
            assert_eq!(output["stuff"], json!({"bar":"eval", "things":123}));
        }
        Ok(())
    }

    #[test]
    fn denest_deep_nested_path() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc level1.level2.level3.value").unwrap();
        let mut subp = DenestProcessor::new(&args)?;
        let val = json!({
            "level1": {
                "level2": {
                    "level3": {
                        "value": "deep_value"
                    }
                }
            },
            "other": "data"
        });

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["value"], "deep_value");
        assert!(result["other"].is_null());
        Ok(())
    }

    #[test]
    fn denest_missing_path() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc nonexistent.path").unwrap();
        let mut subp = DenestProcessor::new(&args)?;
        let val = json!({
            "foo": "bar"
        });

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    #[test]
    fn denest_non_object_input() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar").unwrap();
        let mut subp = DenestProcessor::new(&args)?;
        let val = json!("not an object");

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    #[test]
    fn denest_multiple_paths() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc user.name user.email settings.theme").unwrap();
        let mut subp = DenestProcessor::new(&args)?;
        let val = json!({
            "user": {
                "name": "John Doe",
                "email": "john@example.com",
                "id": 123
            },
            "settings": {
                "theme": "dark",
                "language": "en"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["name"], "John Doe");
        assert_eq!(result["email"], "john@example.com");
        assert_eq!(result["theme"], "dark");
        assert!(result["id"].is_null());
        assert!(result["language"].is_null());
        Ok(())
    }

    #[test]
    fn denest_with_into_empty_result() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc nonexistent.path -i result").unwrap();
        let mut subp = DenestProcessor::new(&args)?;
        let val = json!({
            "foo": "bar"
        });

        let event = Rc::new(RefCell::new(val.clone()));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["foo"], "bar");
        // When no paths are found and into is specified, the original event is returned unchanged
        assert!(result.get("result").is_none() || result["result"].is_null());
        Ok(())
    }

    // RemoveProcessor Tests
    #[test]
    fn remove_single_field() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc password").unwrap();
        let mut subp = RemoveProcessor::new(&args)?;
        let val = json!({
            "username": "john",
            "password": "secret123",
            "email": "john@example.com"
        });

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["username"], "john");
        assert_eq!(result["email"], "john@example.com");
        assert!(result["password"].is_null());
        Ok(())
    }

    #[test]
    fn remove_multiple_fields() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc password secret_key").unwrap();
        let mut subp = RemoveProcessor::new(&args)?;
        let val = json!({
            "username": "john",
            "password": "secret123",
            "email": "john@example.com",
            "secret_key": "abc123",
            "public_data": "visible"
        });

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["username"], "john");
        assert_eq!(result["email"], "john@example.com");
        assert_eq!(result["public_data"], "visible");
        assert!(result["password"].is_null());
        assert!(result["secret_key"].is_null());
        Ok(())
    }

    #[test]
    fn remove_nonexistent_field() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc nonexistent").unwrap();
        let mut subp = RemoveProcessor::new(&args)?;
        let val = json!({
            "username": "john",
            "email": "john@example.com"
        });

        let event = Rc::new(RefCell::new(val.clone()));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(*result, val);
        Ok(())
    }

    #[test]
    fn remove_all_fields() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc username email").unwrap();
        let mut subp = RemoveProcessor::new(&args)?;
        let val = json!({
            "username": "john",
            "email": "john@example.com"
        });

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    #[test]
    fn remove_non_object_input() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc field").unwrap();
        let mut subp = RemoveProcessor::new(&args)?;
        let val = json!("not an object");

        let event = Rc::new(RefCell::new(val.clone()));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);
        assert_eq!(*output[0].borrow(), val);
        Ok(())
    }

    #[test]
    fn remove_from_array_input() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc field").unwrap();
        let mut subp = RemoveProcessor::new(&args)?;
        let val = json!([1, 2, 3]);

        let event = Rc::new(RefCell::new(val.clone()));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);
        assert_eq!(*output[0].borrow(), val);
        Ok(())
    }

    // NestProcessor Tests
    #[test]
    fn nest_test() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc -i here").unwrap();
        let mut subp = NestProcessor::new(&args)?;
        let val = json!({ "foo" : { "bar": "eval", }, });

        let event = Rc::new(RefCell::new(val.clone()));
        let mut output = subp.process(event.clone())?;
        assert_eq!(output.len(), 1);
        if let Some(output_event) = output.pop() {
            let output = output_event.take();
            assert_eq!(output, json!({"here":{"foo":{"bar":"eval"}}}));
        }

        let args = shlex::split("proc foo.bar -i here").unwrap();
        let event = Rc::new(RefCell::new(val.clone()));
        let mut subp = NestProcessor::new(&args)?;
        let mut output = subp.process(event.clone())?;
        assert_eq!(output.len(), 1);
        if let Some(output_event) = output.pop() {
            let output = output_event.take();
            assert_eq!(output, json!({"here":"eval"}));
        }
        Ok(())
    }

    #[test]
    fn nest_default_label() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc").unwrap();
        let mut subp = NestProcessor::new(&args)?;
        let val = json!({
            "key": "value",
            "number": 42
        });

        let event = Rc::new(RefCell::new(val.clone()));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["nest"], val);
        Ok(())
    }

    #[test]
    fn nest_custom_label() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc -i custom_label").unwrap();
        let mut subp = NestProcessor::new(&args)?;
        let val = json!({
            "data": "test"
        });

        let event = Rc::new(RefCell::new(val.clone()));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["custom_label"], val);
        Ok(())
    }

    #[test]
    fn nest_with_jmespath_array() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc items[*].name -i names").unwrap();
        let mut subp = NestProcessor::new(&args)?;
        let val = json!({
            "items": [
                {"name": "item1", "id": 1},
                {"name": "item2", "id": 2},
                {"name": "item3", "id": 3}
            ]
        });

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["names"], json!(["item1", "item2", "item3"]));
        Ok(())
    }

    #[test]
    fn nest_with_jmespath_filter() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc items[?price > `10`] -i expensive_items").unwrap();
        let mut subp = NestProcessor::new(&args)?;
        let val = json!({
            "items": [
                {"name": "cheap", "price": 5},
                {"name": "expensive", "price": 15},
                {"name": "moderate", "price": 10}
            ]
        });

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["expensive_items"], json!([{"name": "expensive", "price": 15}]));
        Ok(())
    }

    #[test]
    fn nest_jmespath_null_result() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc nonexistent.path -i result").unwrap();
        let mut subp = NestProcessor::new(&args)?;
        let val = json!({
            "existing": "data"
        });

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    #[test]
    fn nest_complex_jmespath() -> Result<(), anyhow::Error> {
        let args =
            shlex::split("proc users[?age >= `18`].{name: name, email: email} -i adults").unwrap();
        let mut subp = NestProcessor::new(&args)?;
        let val = json!({
            "users": [
                {"name": "Alice", "age": 25, "email": "alice@example.com"},
                {"name": "Bob", "age": 16, "email": "bob@example.com"},
                {"name": "Charlie", "age": 30, "email": "charlie@example.com"}
            ]
        });

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        let expected = json!([
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Charlie", "email": "charlie@example.com"}
        ]);
        assert_eq!(result["adults"], expected);
        Ok(())
    }

    #[test]
    fn nest_empty_array() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc empty_array -i result").unwrap();
        let mut subp = NestProcessor::new(&args)?;
        let val = json!({
            "empty_array": []
        });

        let event = Rc::new(RefCell::new(val));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["result"], json!([]));
        Ok(())
    }

    #[test]
    fn nest_primitive_values() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc -i wrapped").unwrap();
        let mut subp = NestProcessor::new(&args)?;

        // Test with string
        let string_val = json!("hello world");
        let event = Rc::new(RefCell::new(string_val.clone()));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].borrow()["wrapped"], string_val);

        // Test with number
        let mut subp = NestProcessor::new(&shlex::split("proc -i wrapped").unwrap())?;
        let number_val = json!(42);
        let event = Rc::new(RefCell::new(number_val.clone()));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].borrow()["wrapped"], number_val);

        // Test with boolean
        let mut subp = NestProcessor::new(&shlex::split("proc -i wrapped").unwrap())?;
        let bool_val = json!(true);
        let event = Rc::new(RefCell::new(bool_val.clone()));
        let output = subp.process(event)?;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].borrow()["wrapped"], bool_val);

        Ok(())
    }
}
