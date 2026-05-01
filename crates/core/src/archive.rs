use crate::OrionError;
use core::mem::align_of;
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
    api::high::{HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    de::pooling::Pool,
    rancor::{Error as ArchiveError, Strategy},
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};

pub type DecodeStrategy = Strategy<Pool, ArchiveError>;
pub type DecodeValidator<'a> = HighValidator<'a, ArchiveError>;
const DEFAULT_MAX_LENGTH_PREFIXED_BYTES: usize = 8 * 1024 * 1024;

pub trait ArchiveEncode:
    Sized + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, ArchiveError>>
{
}

impl<T> ArchiveEncode for T where
    T: Sized + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, ArchiveError>>
{
}

pub fn encode_to_vec<T>(value: &T) -> Result<Vec<u8>, OrionError>
where
    T: ArchiveEncode,
{
    let bytes = rkyv::to_bytes::<ArchiveError>(value)
        .map_err(|err| OrionError::Serialization(err.to_string()))?;
    Ok(bytes.as_ref().to_vec())
}

pub fn decode_from_slice<T>(payload: &[u8]) -> Result<T, OrionError>
where
    T: Archive,
    T::Archived: for<'a> CheckBytes<DecodeValidator<'a>> + RkyvDeserialize<T, DecodeStrategy>,
{
    if payload.is_empty() || payload.as_ptr().align_offset(align_of::<T::Archived>()) == 0 {
        return rkyv::from_bytes::<T, ArchiveError>(payload)
            .map_err(|err| OrionError::Serialization(err.to_string()));
    }

    let mut aligned = AlignedVec::<16>::with_capacity(payload.len());
    aligned.extend_from_slice(payload);
    rkyv::from_bytes::<T, ArchiveError>(&aligned)
        .map_err(|err| OrionError::Serialization(err.to_string()))
}

pub fn decode_from_slice_with<T, E, F>(payload: &[u8], map_err: F) -> Result<T, E>
where
    T: Archive,
    T::Archived: for<'a> CheckBytes<DecodeValidator<'a>> + RkyvDeserialize<T, DecodeStrategy>,
    F: FnOnce(String) -> E + Copy,
{
    if payload.is_empty() || payload.as_ptr().align_offset(align_of::<T::Archived>()) == 0 {
        return rkyv::from_bytes::<T, ArchiveError>(payload).map_err(map_archive_error(map_err));
    }

    let mut aligned = AlignedVec::<16>::with_capacity(payload.len());
    aligned.extend_from_slice(payload);
    rkyv::from_bytes::<T, ArchiveError>(&aligned).map_err(map_archive_error(map_err))
}

fn map_archive_error<E, F>(map_err: F) -> impl FnOnce(ArchiveError) -> E
where
    F: FnOnce(String) -> E,
{
    move |err| map_err(err.to_string())
}

pub fn encode_length_prefixed<T, E, F, G>(
    value: &T,
    map_encode_error: F,
    frame_too_large: G,
) -> Result<Vec<u8>, E>
where
    T: ArchiveEncode,
    F: FnOnce(String) -> E,
    G: FnOnce() -> E,
{
    let payload = encode_to_vec(value).map_err(|err| map_encode_error(err.to_string()))?;
    let length = u32::try_from(payload.len()).map_err(|_| frame_too_large())?;

    let mut encoded = Vec::with_capacity(4 + payload.len());
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

pub fn decode_length_prefixed<T, E, F, G, H, I>(
    bytes: &[u8],
    map_decode_error: F,
    incomplete_header: G,
    incomplete_payload: H,
    frame_too_large: I,
) -> Result<(T, &[u8]), E>
where
    T: Archive,
    T::Archived: for<'a> CheckBytes<DecodeValidator<'a>> + RkyvDeserialize<T, DecodeStrategy>,
    F: FnOnce(String) -> E + Copy,
    G: FnOnce() -> E,
    H: FnOnce() -> E,
    I: FnOnce() -> E,
{
    if bytes.len() < 4 {
        return Err(incomplete_header());
    }

    let length = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if length > DEFAULT_MAX_LENGTH_PREFIXED_BYTES {
        return Err(frame_too_large());
    }
    if bytes.len() < 4 + length {
        return Err(incomplete_payload());
    }

    let payload = &bytes[4..4 + length];
    let value = decode_from_slice_with::<T, _, _>(payload, map_decode_error)?;
    Ok((value, &bytes[4 + length..]))
}
