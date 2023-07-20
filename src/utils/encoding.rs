use encoding_rs::{Decoder, DecoderResult, Encoder, EncoderResult};

pub const UTF8_REPLACEMENT_CHARACTER: [u8; 3] = [0xEF, 0xBF, 0xBD];

pub fn utf8_surrocate_escape(input: &[u8], out: &mut Vec<u8>) {
    for b in input {
        out.extend_from_slice(&[0xED, 0xB0 | b >> 6, 0x80 | (b >> 4) & 0x3, b & 0xF]);
    }
}

pub fn utf8_surrogate_unescape(input: &[u8], out: &mut Vec<u8>) -> Result<(), ()> {
    if input.len() % 3 != 0 {
        return Err(());
    }
    let mut i = 0;
    while i < input.len() {
        if input[i] != 0xED || input[i + 1] & 0xFC != 0xB0 || input[i + 2] & 0xD0 != 0x80 {
            return Err(());
        }
        out.push((input[i + 1] & 0x3) << 6 | (input[i + 2] & 0x3F));
        i += 3;
    }
    Ok(())
}

pub fn decode_to_utf8<E>(
    decoder: &mut Decoder,
    input: &[u8],
    replacement_fn: &mut impl FnMut(&[u8], &mut Vec<u8>) -> Result<(), E>,
    output: &mut Vec<u8>,
    last_chunk: bool,
) -> Result<bool, (usize, E)> {
    let mut replacement_called = false;
    let mut read = 0;
    let mut written = output.len();
    let size_guestimate = decoder
        .max_utf8_buffer_length_without_replacement(input.len())
        .unwrap_or(input.len());
    output.reserve(size_guestimate);
    loop {
        let output_buffer = unsafe {
            // SAFETY: we create an uninitialized slice &[u8] here.
            // decode_to_utf8_without_replacement will only write,
            // and not read from it. While the UB-ness of this is apparently undecided,
            // this library has no other api and uses this construct internally a lot too,
            // so if we're gonna use it we might aswell ...
            std::slice::from_raw_parts_mut(
                output.as_mut_ptr().add(written),
                output.capacity() - written,
            )
        };

        let (decoder_result, decoder_read, decoder_written) =
            decoder.decode_to_utf8_without_replacement(&input[read..], output_buffer, last_chunk);
        written += decoder_written;
        read += decoder_read;

        match decoder_result {
            DecoderResult::InputEmpty => unsafe {
                // SAFETY:
                // we fully trust decode_to_utf8_without_replacement to have
                // written those bytes
                output.set_len(written);
                return Ok(replacement_called);
            },
            DecoderResult::OutputFull => {
                output.reserve(output.capacity());
                continue;
            }
            DecoderResult::Malformed(malformed_seq_len, extra_bytes_read_after_malformed_seq) => {
                let malformed_seq_start = read
                    - malformed_seq_len as usize
                    - extra_bytes_read_after_malformed_seq as usize;
                let malformed_seq_end = read - extra_bytes_read_after_malformed_seq as usize;
                replacement_fn(&input[malformed_seq_start..malformed_seq_end], output)
                    .map_err(|e| (decoder_read, e))?;
                replacement_called = true;
            }
        }
    }
}

pub fn encode_from_utf8<'a, E>(
    encoder: &mut Encoder,
    input: &str,
    replacement_fn: &'a mut impl FnMut(char, &mut Vec<u8>) -> Result<(), E>,
    output: &mut Vec<u8>,
    last_chunk: bool,
) -> Result<bool, (usize, E)> {
    let mut replacement_called = false;
    let mut read = 0;
    let mut written = output.len();
    let size_guestimate = encoder
        .max_buffer_length_from_utf8_if_no_unmappables(input.len())
        .unwrap_or(input.len());
    output.reserve(size_guestimate);
    loop {
        let output_buffer = unsafe {
            // SAFETY see decode_to_utf8 for the same construct
            std::slice::from_raw_parts_mut(
                output.as_mut_ptr().add(written),
                output.capacity() - written,
            )
        };

        let (encoder_result, encoder_read, encoder_written) =
            encoder.encode_from_utf8_without_replacement(&input[read..], output_buffer, last_chunk);
        written += encoder_written;
        read += encoder_read;

        match encoder_result {
            EncoderResult::InputEmpty => unsafe {
                // SAFETY: same as in decode_to_utf8
                output.set_len(written);
                return Ok(replacement_called);
            },
            EncoderResult::OutputFull => {
                output.reserve(output.capacity());
                continue;
            }
            EncoderResult::Unmappable(unmappable_char) => {
                replacement_fn(unmappable_char, output).map_err(|e| (encoder_read, e))?;
                replacement_called = true;
            }
        }
    }
}
