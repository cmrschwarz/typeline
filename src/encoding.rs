use encoding_rs::{Decoder, DecoderResult, Encoder, EncoderResult};

pub fn decode_to_utf8(
    decoder: &mut Decoder,
    input: &[u8],
    replacement_fn: &mut impl Fn(&[u8], &mut Vec<u8>),
    output: &mut Vec<u8>,
    last_chunk: bool,
) {
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
                return;
            },
            DecoderResult::OutputFull => {
                output.reserve(output.capacity() * 2);
                continue;
            }
            DecoderResult::Malformed(malformed_seq_len, extra_bytes_read_after_malformed_seq) => {
                let malformed_seq_start = read
                    - malformed_seq_len as usize
                    - extra_bytes_read_after_malformed_seq as usize;
                let malformed_seq_end = read - extra_bytes_read_after_malformed_seq as usize;
                replacement_fn(&input[malformed_seq_start..malformed_seq_end], output);
            }
        }
    }
}

pub fn encode_from_utf8<'a>(
    encoder: &mut Encoder,
    input: &str,
    replacement_fn: &'a mut impl Fn(char, &mut Vec<u8>) -> Result<(), &'a str>,
    output: &mut Vec<u8>,
    last_chunk: bool,
) -> Result<(), &'a str> {
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

        let (encoder_result, decoder_read, decoder_written) =
            encoder.encode_from_utf8_without_replacement(&input[read..], output_buffer, last_chunk);
        written += decoder_written;
        read += decoder_read;

        match encoder_result {
            EncoderResult::InputEmpty => unsafe {
                // SAFETY: same as in decode_to_utf8
                output.set_len(written);
                return Ok(());
            },
            EncoderResult::OutputFull => {
                output.reserve(output.capacity() * 2);
                continue;
            }
            EncoderResult::Unmappable(unmappable_char) => {
                replacement_fn(unmappable_char, output)?;
            }
        }
    }
}
