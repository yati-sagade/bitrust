error_chain! { // comes from crate error_chain
    errors {
        InvalidData(msg: String) {
            description("Invalid or corrupted data was encountered"),
            display("Invalid or corrupted data was encountered: '{}'", msg)
        }
        InvalidRead(msg: String) {
            description("Read was performed where none was possible"),
            display("Invalid read: {}", msg)
        }
        InvalidFileKind(msg: String) {
            description("Invalid/unknown file kind"),
            display("Invalid file kind: {}", msg)
        }
    }
    foreign_links {
        Io(::std::io::Error) #[cfg(unix)];
        Proto(::protobuf::ProtobufError);
    }
}
