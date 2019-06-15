error_chain! { // comes from crate error_chain
    errors {
        InvalidData(msg: String) {
            description("Invalid or corrupted data was encountered"),
            display("Invalid or corrupted data was encountered: '{}'", msg)
        }
    }
}
