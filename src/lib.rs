use lazy_static::lazy_static;

lazy_static! {
    pub static ref VERBOSE: bool = std::env::var("EVENTWINNOWER_VERBOSE").is_ok();
}

#[macro_export]
macro_rules! debug_log {
    ($($arg:tt)*) => {
        if *$crate::VERBOSE {
            eprintln!($($arg)*);
        }
    };
}

pub mod processors;
pub mod workflow;
