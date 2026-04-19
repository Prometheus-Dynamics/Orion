use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Expr, Fields, Item, ItemEnum, ItemStruct, LitStr, MetaNameValue, Token,
    parse::Parse, parse_macro_input, punctuated::Punctuated, spanned::Spanned,
};

#[proc_macro_attribute]
pub fn orion_runtime_type(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_type_def(attr, item, TypeKind::Runtime)
}

#[proc_macro_attribute]
pub fn orion_resource_type(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_type_def(attr, item, TypeKind::Resource)
}

#[proc_macro_derive(OrionExecutor, attributes(orion_executor))]
pub fn derive_orion_executor(input: TokenStream) -> TokenStream {
    expand_orion_descriptor(
        &parse_macro_input!(input as DeriveInput),
        DescriptorKind::Executor,
    )
}

#[proc_macro_derive(OrionProvider, attributes(orion_provider))]
pub fn derive_orion_provider(input: TokenStream) -> TokenStream {
    expand_orion_descriptor(
        &parse_macro_input!(input as DeriveInput),
        DescriptorKind::Provider,
    )
}

#[derive(Clone, Copy)]
enum TypeKind {
    Runtime,
    Resource,
}

#[derive(Clone, Copy)]
enum DescriptorKind {
    Executor,
    Provider,
}

fn expand_type_def(attr: TokenStream, item: TokenStream, kind: TypeKind) -> TokenStream {
    let type_literal = parse_macro_input!(attr as LitStr);
    let item = parse_macro_input!(item as Item);

    match item {
        Item::Struct(item_struct) => expand_struct(&item_struct, &type_literal, kind),
        Item::Enum(item_enum) => expand_enum(&item_enum, &type_literal, kind),
        other => syn::Error::new(
            other.span(),
            "Orion type declaration macros support only structs or enums",
        )
        .to_compile_error()
        .into(),
    }
}

fn expand_struct(item: &ItemStruct, type_literal: &LitStr, kind: TypeKind) -> TokenStream {
    let ident = &item.ident;
    let (trait_path, constructor_name, constructor_type) = match kind {
        TypeKind::Runtime => (
            quote!(::orion_core::RuntimeTypeDef),
            quote!(runtime_type),
            quote!(::orion_core::RuntimeType),
        ),
        TypeKind::Resource => (
            quote!(::orion_core::ResourceTypeDef),
            quote!(resource_type),
            quote!(::orion_core::ResourceType),
        ),
    };

    quote! {
        #item

        impl #trait_path for #ident {
            const TYPE: &'static str = #type_literal;
        }

        impl #ident {
            pub fn #constructor_name() -> #constructor_type {
                <#constructor_type>::of::<Self>()
            }
        }
    }
    .into()
}

fn expand_enum(item: &ItemEnum, type_literal: &LitStr, kind: TypeKind) -> TokenStream {
    let ident = &item.ident;
    let (trait_path, constructor_name, constructor_type) = match kind {
        TypeKind::Runtime => (
            quote!(::orion_core::RuntimeTypeDef),
            quote!(runtime_type),
            quote!(::orion_core::RuntimeType),
        ),
        TypeKind::Resource => (
            quote!(::orion_core::ResourceTypeDef),
            quote!(resource_type),
            quote!(::orion_core::ResourceType),
        ),
    };

    quote! {
        #item

        impl #trait_path for #ident {
            const TYPE: &'static str = #type_literal;
        }

        impl #ident {
            pub fn #constructor_name() -> #constructor_type {
                <#constructor_type>::of::<Self>()
            }
        }
    }
    .into()
}

struct OrionDescriptorArgs {
    id: LitStr,
    type_literals: Vec<LitStr>,
}

impl Parse for OrionDescriptorArgs {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let args = Punctuated::<MetaNameValue, Token![,]>::parse_terminated(input)?;
        let mut id = None;
        let mut type_literals = None;

        for arg in args {
            if arg.path.is_ident("id") {
                match arg.value {
                    Expr::Lit(expr) => match expr.lit {
                        syn::Lit::Str(value) => id = Some(value),
                        other => {
                            return Err(syn::Error::new(
                                other.span(),
                                "id must be a string literal",
                            ));
                        }
                    },
                    other => {
                        return Err(syn::Error::new(other.span(), "id must be a string literal"));
                    }
                }
            } else if arg.path.is_ident("runtime_types") || arg.path.is_ident("resource_types") {
                match arg.value {
                    Expr::Array(array) => {
                        let mut values = Vec::new();
                        for expr in array.elems {
                            match expr {
                                Expr::Lit(expr) => match expr.lit {
                                    syn::Lit::Str(value) => values.push(value),
                                    other => {
                                        return Err(syn::Error::new(
                                            other.span(),
                                            "type list entries must be string literals",
                                        ));
                                    }
                                },
                                other => {
                                    return Err(syn::Error::new(
                                        other.span(),
                                        "type list entries must be string literals",
                                    ));
                                }
                            }
                        }
                        type_literals = Some(values);
                    }
                    other => {
                        return Err(syn::Error::new(
                            other.span(),
                            "type list must be an array of string literals",
                        ));
                    }
                }
            }
        }

        Ok(Self {
            id: id.ok_or_else(|| syn::Error::new(input.span(), "missing `id = \"...\"`"))?,
            type_literals: type_literals.ok_or_else(|| {
                syn::Error::new(
                    input.span(),
                    "missing `runtime_types = [..]` or `resource_types = [..]`",
                )
            })?,
        })
    }
}

fn expand_orion_descriptor(input: &DeriveInput, kind: DescriptorKind) -> TokenStream {
    let ident = &input.ident;
    let attrs_name = match kind {
        DescriptorKind::Executor => "orion_executor",
        DescriptorKind::Provider => "orion_provider",
    };
    let attr = match input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident(attrs_name))
    {
        Some(attr) => attr,
        None => {
            return syn::Error::new(
                input.span(),
                format!("missing #[{attrs_name}(id = \"...\", ...)] attribute"),
            )
            .to_compile_error()
            .into();
        }
    };
    let args = match attr.parse_args::<OrionDescriptorArgs>() {
        Ok(args) => args,
        Err(err) => return err.to_compile_error().into(),
    };

    let has_node_id = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => fields
                .named
                .iter()
                .any(|field| field.ident.as_ref().is_some_and(|ident| ident == "node_id")),
            _ => false,
        },
        _ => false,
    };

    if !has_node_id {
        return syn::Error::new(
            input.span(),
            "Orion descriptor derives require a named `node_id` field",
        )
        .to_compile_error()
        .into();
    }

    let id_literal = args.id;
    let type_literals = args.type_literals;

    let expanded = match kind {
        DescriptorKind::Executor => {
            let runtime_types = type_literals
                .iter()
                .map(|lit| quote!(::orion_core::RuntimeType::new(#lit)));
            quote! {
                impl ::orion_runtime::ExecutorDescriptor for #ident {
                    fn executor_record(&self) -> ::orion_control_plane::ExecutorRecord {
                        ::orion_control_plane::ExecutorRecord::builder(#id_literal, self.node_id.clone())
                            #(.runtime_type(#runtime_types))*
                            .build()
                    }
                }
            }
        }
        DescriptorKind::Provider => {
            let resource_types = type_literals
                .iter()
                .map(|lit| quote!(::orion_core::ResourceType::new(#lit)));
            quote! {
                impl ::orion_runtime::ProviderDescriptor for #ident {
                    fn provider_record(&self) -> ::orion_control_plane::ProviderRecord {
                        ::orion_control_plane::ProviderRecord::builder(#id_literal, self.node_id.clone())
                            #(.resource_type(#resource_types))*
                            .build()
                    }
                }
            }
        }
    };

    expanded.into()
}
