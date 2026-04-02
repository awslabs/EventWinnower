use proc_macro::TokenStream;
use quote::{format_ident, quote};

//Macro for ProcessorInit Trait
//https://doc.rust-lang.org/book/ch19-06-macros.html#how-to-write-a-custom-derive-macro
//https://docs.rs/quote/latest/quote/macro.quote.html#constructing-identifiers
#[proc_macro_derive(AsyncProcessorInit)]
pub fn async_processor_init_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_async_processor_init_macro(&ast)
}

fn impl_async_processor_init_macro(ast: &syn::DeriveInput) -> TokenStream {
    let initname = format_ident!("{}Init", &ast.ident);
    let name = &ast.ident;
    let gen = quote! {
        pub struct #initname;
        impl #initname {
            pub fn new() -> Box<dyn ProcessorInit> {
                Box::new(Self{})
            }
        }
        #[::async_trait::async_trait(?Send)]
        impl ProcessorInit for #initname {
            async fn init(&self, argv:&[String], instance_id: usize) -> Result<ProcessorType, anyhow::Error> {
                Ok(ProcessorType::Async(Box::new(#name::new_with_instance_id(argv, instance_id).await?)))
            }
            fn get_simple_description(&self) -> Option<String> {
                #name::get_simple_description()
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(AsyncBatchChildrenProcessorInit)]
pub fn async_batch_children_processor_init_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_async_batch_children_processor_init_macro(&ast)
}

fn impl_async_batch_children_processor_init_macro(ast: &syn::DeriveInput) -> TokenStream {
    let initname = format_ident!("{}Init", &ast.ident);
    let name = &ast.ident;
    let gen = quote! {
        pub struct #initname;
        impl #initname {
            pub fn new() -> Box<dyn ProcessorInit> {
                Box::new(Self{})
            }
        }
        #[::async_trait::async_trait(?Send)]
        impl ProcessorInit for #initname {
            async fn init(&self, argv:&[String], instance_id: usize) -> Result<ProcessorType, anyhow::Error> {
                Ok(ProcessorType::AsyncBatchChildren(Box::new(#name::new_with_instance_id(argv, instance_id).await?)))
            }
            fn get_simple_description(&self) -> Option<String> {
                #name::get_simple_description()
            }
        }
    };
    gen.into()
}


#[proc_macro_derive(SerialProcessorInit)]
pub fn serial_processor_init_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_serial_processor_init_macro(&ast)
}

fn impl_serial_processor_init_macro(ast: &syn::DeriveInput) -> TokenStream {
    let initname = format_ident!("{}Init", &ast.ident);
    let name = &ast.ident;
    let gen = quote! {
        pub struct #initname;
        impl #initname {
            pub fn new() -> Box<dyn ProcessorInit> {
                Box::new(Self{})
            }
        }
        #[::async_trait::async_trait(?Send)]
        impl ProcessorInit for #initname {
            async fn init(&self, argv:&[String], instance_id: usize) -> Result<ProcessorType, anyhow::Error> {
                Ok(ProcessorType::Serial(Box::new(#name::new_with_instance_id(argv, instance_id)?)))
            }
            fn get_simple_description(&self) -> Option<String> {
                #name::get_simple_description()
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(AsyncSourceProcessorInit)]
pub fn async_source_processor_init_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_async_source_processor_init_macro(&ast)
}

fn impl_async_source_processor_init_macro(ast: &syn::DeriveInput) -> TokenStream {
    let initname = format_ident!("{}Init", &ast.ident);
    let name = &ast.ident;
    let gen = quote! {
        pub struct #initname;
        impl #initname {
            pub fn new() -> Box<dyn ProcessorInit> {
                Box::new(Self{})
            }
        }

        #[::async_trait::async_trait(?Send)]
        impl ProcessorInit for #initname {
            async fn init(&self, argv:&[String], instance_id: usize) -> Result<ProcessorType, anyhow::Error> {
                Ok(ProcessorType::AsyncSource(Box::new(#name::new_with_instance_id(argv, instance_id).await?)))
            }
            fn get_simple_description(&self) -> Option<String> {
                #name::get_simple_description()
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(SerialSourceProcessorInit)]
pub fn serial_source_processor_init_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_serial_source_processor_init_macro(&ast)
}

fn impl_serial_source_processor_init_macro(ast: &syn::DeriveInput) -> TokenStream {
    let initname = format_ident!("{}Init", &ast.ident);
    let name = &ast.ident;
    let gen = quote! {
        pub struct #initname;
        impl #initname {
            pub fn new() -> Box<dyn ProcessorInit> {
                Box::new(Self{})
            }
        }

        #[::async_trait::async_trait(?Send)]
        impl ProcessorInit for #initname {
            async fn init(&self, argv:&[String], instance_id: usize) -> Result<ProcessorType, anyhow::Error> {
                Ok(ProcessorType::SerialSource(Box::new(#name::new_with_instance_id(argv, instance_id)?)))
            }
            fn get_simple_description(&self) -> Option<String> {
                #name::get_simple_description()
            }
        }
    };
    gen.into()
}
