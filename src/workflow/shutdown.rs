use std::sync::atomic::{AtomicBool, Ordering};
pub static GLOBAL_SHUTDOWN: AtomicBool = AtomicBool::new(false);

pub async fn init_control_c_shutdown() {
    let mut stop_cnt = 0;

    ctrlc::set_handler(move || {
        eprintln!("received Control+C");

        GLOBAL_SHUTDOWN.store(true, Ordering::Relaxed);
        stop_cnt += 1;
        if stop_cnt > 3 {
            eprintln!("Force Stopping");
            std::process::exit(-1);
        }
    })
    .expect("Error setting signal handler");
}
