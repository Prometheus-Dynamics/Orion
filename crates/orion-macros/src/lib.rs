use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Expr, Field, Fields, Item, ItemEnum, ItemStruct, LitStr, Meta,
    MetaNameValue, Token, Type, Variant, parse::Parse, parse_macro_input, punctuated::Punctuated,
    spanned::Spanned,
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

#[proc_macro_derive(OrionConfigDecode, attributes(orion))]
pub fn derive_orion_config_decode(input: TokenStream) -> TokenStream {
    expand_orion_config_decode(&parse_macro_input!(input as DeriveInput))
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
            quote!(::orion::RuntimeTypeDef),
            quote!(runtime_type),
            quote!(::orion::RuntimeType),
        ),
        TypeKind::Resource => (
            quote!(::orion::ResourceTypeDef),
            quote!(resource_type),
            quote!(::orion::ResourceType),
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
            quote!(::orion::RuntimeTypeDef),
            quote!(runtime_type),
            quote!(::orion::RuntimeType),
        ),
        TypeKind::Resource => (
            quote!(::orion::ResourceTypeDef),
            quote!(resource_type),
            quote!(::orion::ResourceType),
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
                .map(|lit| quote!(::orion::RuntimeType::new(#lit)));
            quote! {
                impl ::orion::runtime::ExecutorDescriptor for #ident {
                    fn executor_record(&self) -> ::orion::control_plane::ExecutorRecord {
                        ::orion::control_plane::ExecutorRecord::builder(#id_literal, self.node_id.clone())
                            #(.runtime_type(#runtime_types))*
                            .build()
                    }
                }
            }
        }
        DescriptorKind::Provider => {
            let resource_types = type_literals
                .iter()
                .map(|lit| quote!(::orion::ResourceType::new(#lit)));
            quote! {
                impl ::orion::runtime::ProviderDescriptor for #ident {
                    fn provider_record(&self) -> ::orion::control_plane::ProviderRecord {
                        ::orion::control_plane::ProviderRecord::builder(#id_literal, self.node_id.clone())
                            #(.resource_type(#resource_types))*
                            .build()
                    }
                }
            }
        }
    };

    expanded.into()
}

fn expand_orion_config_decode(input: &DeriveInput) -> TokenStream {
    let ident = &input.ident;
    let decode_body = match &input.data {
        Data::Struct(data) => match expand_struct_config_decode_body(&data.fields) {
            Ok(body) => body,
            Err(err) => return err.to_compile_error().into(),
        },
        Data::Enum(data) => match expand_enum_config_decode_body(input, &data.variants) {
            Ok(body) => body,
            Err(err) => return err.to_compile_error().into(),
        },
        _ => {
            return syn::Error::new(
                input.span(),
                "OrionConfigDecode only supports structs or enums",
            )
            .to_compile_error()
            .into();
        }
    };

    quote! {
        impl ::core::convert::TryFrom<&::orion::control_plane::WorkloadConfig> for #ident {
            type Error = ::orion::control_plane::ConfigDecodeError;

            fn try_from(config: &::orion::control_plane::WorkloadConfig) -> ::core::result::Result<Self, Self::Error> {
                ::core::convert::TryFrom::try_from(&config.payload)
            }
        }

        impl ::core::convert::TryFrom<&::orion::control_plane::ResourceConfigState> for #ident {
            type Error = ::orion::control_plane::ConfigDecodeError;

            fn try_from(config: &::orion::control_plane::ResourceConfigState) -> ::core::result::Result<Self, Self::Error> {
                ::core::convert::TryFrom::try_from(&config.payload)
            }
        }

        impl ::core::convert::TryFrom<&::std::collections::BTreeMap<::std::string::String, ::orion::control_plane::TypedConfigValue>> for #ident {
            type Error = ::orion::control_plane::ConfigDecodeError;

            fn try_from(
                payload: &::std::collections::BTreeMap<::std::string::String, ::orion::control_plane::TypedConfigValue>
            ) -> ::core::result::Result<Self, Self::Error> {
                let config = ::orion::control_plane::ConfigMapRef::new(payload);
                #decode_body
            }
        }
    }
    .into()
}

fn expand_struct_config_decode_body(fields: &Fields) -> syn::Result<proc_macro2::TokenStream> {
    let named = match fields {
        Fields::Named(fields) => &fields.named,
        other => {
            return Err(syn::Error::new(
                other.span(),
                "OrionConfigDecode only supports structs with named fields",
            ));
        }
    };

    let mut field_inits = Vec::new();
    for field in named {
        field_inits.push(expand_config_decode_field(field)?);
    }

    Ok(quote! {
        Ok(Self {
            #(#field_inits,)*
        })
    })
}

fn expand_enum_config_decode_body(
    input: &DeriveInput,
    variants: &syn::punctuated::Punctuated<Variant, Token![,]>,
) -> syn::Result<proc_macro2::TokenStream> {
    let tag_field = parse_orion_attr_string(&input.attrs, "tag")?.ok_or_else(|| {
        syn::Error::new(
            input.span(),
            "OrionConfigDecode enums require #[orion(tag = \"...\")]",
        )
    })?;
    let tag_field_lit = LitStr::new(&tag_field, input.span());

    let mut match_arms = Vec::new();
    for variant in variants {
        let variant_ident = &variant.ident;
        let variant_tag = parse_orion_attr_string(&variant.attrs, "tag")?.ok_or_else(|| {
            syn::Error::new(
                variant.span(),
                "OrionConfigDecode enum variants require #[orion(tag = \"...\")]",
            )
        })?;
        let variant_tag_lit = LitStr::new(&variant_tag, variant.span());
        let variant_expr = match &variant.fields {
            Fields::Unit => quote!(Self::#variant_ident),
            Fields::Named(fields) => {
                let mut field_inits = Vec::new();
                for field in &fields.named {
                    field_inits.push(expand_config_decode_field(field)?);
                }
                quote!(Self::#variant_ident { #(#field_inits,)* })
            }
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    "OrionConfigDecode enum variants only support unit or named fields",
                ));
            }
        };
        match_arms.push(quote! {
            #variant_tag_lit => Ok(#variant_expr)
        });
    }

    Ok(quote! {
        match config.required_string(#tag_field_lit)? {
            #(#match_arms,)*
            other => Err(::orion::control_plane::ConfigDecodeError::InvalidValue {
                field: #tag_field_lit.to_owned(),
                message: format!("unsupported tag '{other}'"),
            }),
        }
    })
}

fn expand_config_decode_field(field: &Field) -> syn::Result<proc_macro2::TokenStream> {
    let ident = field
        .ident
        .as_ref()
        .ok_or_else(|| syn::Error::new(field.span(), "expected named field"))?;
    if let Some(prefix) = parse_orion_attr_string(&field.attrs, "prefix")? {
        let access = expand_indexed_group_access(&field.ty, &prefix, field.span())?;
        return Ok(quote! {
            #ident: #access
        });
    }
    let path = parse_config_field_path(field)?.unwrap_or_else(|| ident.to_string());
    let access = expand_config_value_access(&field.ty, &path, field.span())?;
    Ok(quote! {
        #ident: #access
    })
}

fn parse_config_field_path(field: &Field) -> syn::Result<Option<String>> {
    parse_orion_attr_string(&field.attrs, "path")
}

fn expand_config_value_access(
    ty: &Type,
    path: &str,
    span: proc_macro2::Span,
) -> syn::Result<proc_macro2::TokenStream> {
    if let Some(inner) = option_inner_type(ty) {
        return expand_optional_config_value_access(inner, path, span);
    }

    if type_matches(ty, "bool") {
        let path = LitStr::new(path, span);
        return Ok(quote! { config.required_bool(#path)? });
    }
    if type_matches(ty, "i64") {
        let path = LitStr::new(path, span);
        return Ok(quote! { config.required_int(#path)? });
    }
    if type_matches(ty, "u64") {
        let path = LitStr::new(path, span);
        return Ok(quote! { config.required_uint(#path)? });
    }
    if type_matches(ty, "String") {
        let path = LitStr::new(path, span);
        return Ok(quote! { config.required_string(#path)?.to_owned() });
    }
    if vec_u8_inner_type(ty) {
        let path = LitStr::new(path, span);
        return Ok(quote! { config.required_bytes(#path)?.to_vec() });
    }

    Err(syn::Error::new(
        ty.span(),
        "unsupported field type for OrionConfigDecode; supported types are bool, i64, u64, String, Vec<u8>, and Option<...> of those types",
    ))
}

fn expand_optional_config_value_access(
    ty: &Type,
    path: &str,
    span: proc_macro2::Span,
) -> syn::Result<proc_macro2::TokenStream> {
    let path = LitStr::new(path, span);
    if type_matches(ty, "bool") {
        return Ok(quote! { config.optional_bool(#path)? });
    }
    if type_matches(ty, "i64") {
        return Ok(quote! { config.optional_int(#path)? });
    }
    if type_matches(ty, "u64") {
        return Ok(quote! { config.optional_uint(#path)? });
    }
    if type_matches(ty, "String") {
        return Ok(quote! { config.optional_string(#path)?.map(::std::borrow::ToOwned::to_owned) });
    }
    if vec_u8_inner_type(ty) {
        return Ok(quote! { config.optional_bytes(#path)?.map(|value| value.to_vec()) });
    }

    Err(syn::Error::new(
        ty.span(),
        "unsupported Option<T> field type for OrionConfigDecode",
    ))
}

fn expand_indexed_group_access(
    ty: &Type,
    prefix: &str,
    span: proc_macro2::Span,
) -> syn::Result<proc_macro2::TokenStream> {
    let inner = vec_inner_type(ty).ok_or_else(|| {
        syn::Error::new(ty.span(), "indexed-group decode requires a Vec<T> field")
    })?;
    if type_matches(inner, "u8") {
        return Err(syn::Error::new(
            ty.span(),
            "indexed-group decode does not support Vec<u8>; use #[orion(path = ...)] for bytes fields",
        ));
    }
    let prefix = LitStr::new(prefix, span);
    Ok(quote! {{
        let mut values = ::std::vec::Vec::new();
        for index in config.indexed_group_indices(#prefix) {
            let group_payload = config.indexed_group_payload(#prefix, index);
            values.push(<#inner as ::core::convert::TryFrom<&::std::collections::BTreeMap<::std::string::String, ::orion::control_plane::TypedConfigValue>>>::try_from(&group_payload)?);
        }
        values
    }})
}

fn option_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let syn::GenericArgument::Type(inner) = args.args.first()? else {
        return None;
    };
    Some(inner)
}

fn vec_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Vec" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let syn::GenericArgument::Type(inner) = args.args.first()? else {
        return None;
    };
    Some(inner)
}

fn vec_u8_inner_type(ty: &Type) -> bool {
    matches!(vec_inner_type(ty), Some(inner) if type_matches(inner, "u8"))
}

fn type_matches(ty: &Type, expected: &str) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == expected)
}

fn parse_orion_attr_string(attrs: &[syn::Attribute], key: &str) -> syn::Result<Option<String>> {
    for attr in attrs {
        if !attr.path().is_ident("orion") {
            continue;
        }
        match &attr.meta {
            Meta::List(list) => {
                let nested =
                    list.parse_args_with(Punctuated::<MetaNameValue, Token![,]>::parse_terminated)?;
                for meta in nested {
                    if meta.path.is_ident(key) {
                        return match meta.value {
                            Expr::Lit(expr) => match expr.lit {
                                syn::Lit::Str(value) => Ok(Some(value.value())),
                                other => Err(syn::Error::new(
                                    other.span(),
                                    format!("{key} must be a string literal"),
                                )),
                            },
                            other => Err(syn::Error::new(
                                other.span(),
                                format!("{key} must be a string literal"),
                            )),
                        };
                    }
                }
            }
            other => {
                return Err(syn::Error::new(other.span(), "expected #[orion(...)]"));
            }
        }
    }
    Ok(None)
}
