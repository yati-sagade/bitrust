#[macro_use]
use error_chain;

error_chain! {
    errors {
        InvalidData(msg: String) {
            description("Invalid or corrupted data was encountered"),
            display("Invalid or corrupted data was encountered: '{}'", msg)
        }
    }
}

