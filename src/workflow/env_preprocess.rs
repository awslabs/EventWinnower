use logos::Logos;
use std::env;

use anyhow::Result;

#[derive(Logos, Debug, PartialEq)]
#[logos(skip r"[ \t\f]+")]
enum EnvDetect<'a> {
    #[regex("\\$\\{[a-zA-Z0-9_]*\\}", |lex| {let len = lex.slice().len(); &lex.slice()[2..(len-1)] })]
    Environment(&'a str),
}

pub fn env_preprocess(workflow_string: &str) -> Result<String, anyhow::Error> {
    let mut output = String::new();
    let mut lexer = EnvDetect::lexer(workflow_string);

    let mut start = 0;

    while let Some(tok) = lexer.next() {
        let _pre = lexer.slice();

        if let Ok(EnvDetect::Environment(ename)) = tok {
            let cursor = lexer.span();
            if cursor.start > start {
                let pre = &lexer.source()[start..cursor.start];
                output.push_str(pre);
            }
            if let Ok(val) = env::var(ename) {
                debug_log!("Environment variable substituted: {ename}");
                output.push_str(&val);
            } else {
                debug_log!("Environment {ename} not found, ignoring");
            }
            start = cursor.end;
        }
    }
    let remainder = &lexer.source()[start..];
    output.push_str(remainder);

    debug_log!("Environment Preprocesser completed");

    Ok(output)
}
