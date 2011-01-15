//! Bounded RESP2 request framing. SQL and VOS source syntax belongs to Oak.

/// Resource limits for a single command, independent of pipeline length.
#[derive(Clone, Copy, Debug)]
pub struct RequestLimits {
    /// Maximum encoded bytes in one command.
    pub max_frame_bytes: usize,
    /// Maximum command and argument count.
    pub max_arguments: usize,
    /// Maximum bytes in one binary argument.
    pub max_bulk_bytes: usize,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self { max_frame_bytes: 1_048_576, max_arguments: 1024, max_bulk_bytes: 1_048_576 }
    }
}

/// One complete command borrowing its binary arguments from the input.
#[derive(Debug, PartialEq, Eq)]
pub struct Request<'input> {
    /// Command name followed by its arguments, without UTF-8 conversion.
    pub arguments: Vec<&'input [u8]>,
    /// Encoded bytes consumed, leaving subsequent pipeline frames untouched.
    pub consumed: usize,
}

/// A malformed or resource-exhausting request, requiring connection rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// Commands must be nonempty arrays of bulk strings.
    UnexpectedType,
    /// A length is negative, nondecimal, empty, or overflowing.
    InvalidLength,
    /// A line or bulk payload does not end in CRLF.
    InvalidTerminator,
    /// The array contains no command name.
    EmptyCommand,
    /// Encoded command exceeds the configured byte limit.
    FrameTooLarge,
    /// Argument count exceeds the configured limit.
    TooManyArguments,
    /// A bulk argument exceeds the configured limit.
    BulkTooLarge,
}

/// Encodes one RESP2 command as a bulk-string array.
pub fn encode_request(arguments: &[&[u8]], limits: RequestLimits) -> Result<Vec<u8>, FrameError> {
    if arguments.is_empty() {
        return Err(FrameError::EmptyCommand);
    }
    if arguments.len() > limits.max_arguments {
        return Err(FrameError::TooManyArguments);
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(format!("*{}\r\n", arguments.len()).as_bytes());
    for argument in arguments {
        if argument.len() > limits.max_bulk_bytes {
            return Err(FrameError::BulkTooLarge);
        }
        encoded.extend_from_slice(format!("${}\r\n", argument.len()).as_bytes());
        encoded.extend_from_slice(argument);
        encoded.extend_from_slice(b"\r\n");
    }
    if encoded.len() > limits.max_frame_bytes {
        return Err(FrameError::FrameTooLarge);
    }
    Ok(encoded)
}

/// Decodes the first RESP2 command or returns `None` for an incomplete frame.
/// This is wire framing, not a language syntax frontend or command binder.
pub fn decode_request(input: &[u8], limits: RequestLimits) -> Result<Option<Request<'_>>, FrameError> {
    let bounded = &input[..input.len().min(limits.max_frame_bytes)];
    let decoded = decode_bounded(bounded, limits)?;
    if decoded.is_none() && input.len() >= limits.max_frame_bytes {
        return Err(FrameError::FrameTooLarge);
    }
    Ok(decoded)
}

fn decode_bounded(input: &[u8], limits: RequestLimits) -> Result<Option<Request<'_>>, FrameError> {
    let mut offset = 0;
    let Some(count) = read_length(input, &mut offset, b'*')?
    else {
        return Ok(None);
    };
    if count == 0 {
        return Err(FrameError::EmptyCommand);
    }
    if count > limits.max_arguments {
        return Err(FrameError::TooManyArguments);
    }
    let mut arguments = Vec::new();
    for _ in 0..count {
        let Some(length) = read_length(input, &mut offset, b'$')?
        else {
            return Ok(None);
        };
        if length > limits.max_bulk_bytes {
            return Err(FrameError::BulkTooLarge);
        }
        let end = offset.checked_add(length).ok_or(FrameError::FrameTooLarge)?;
        let consumed = end.checked_add(2).ok_or(FrameError::FrameTooLarge)?;
        if consumed > limits.max_frame_bytes {
            return Err(FrameError::FrameTooLarge);
        }
        if input.len() < consumed {
            return Ok(None);
        }
        if &input[end..consumed] != b"\r\n" {
            return Err(FrameError::InvalidTerminator);
        }
        arguments.push(&input[offset..end]);
        offset = consumed;
    }
    Ok(Some(Request { arguments, consumed: offset }))
}

fn read_length(input: &[u8], offset: &mut usize, marker: u8) -> Result<Option<usize>, FrameError> {
    let Some(first) = input.get(*offset)
    else {
        return Ok(None);
    };
    if *first != marker {
        return Err(FrameError::UnexpectedType);
    }
    let mut cursor = *offset + 1;
    let digits_start = cursor;
    let mut value = 0usize;
    while let Some(byte) = input.get(cursor) {
        if *byte == b'\r' {
            if cursor == digits_start {
                return Err(FrameError::InvalidLength);
            }
            let Some(next) = input.get(cursor + 1)
            else {
                return Ok(None);
            };
            if *next != b'\n' {
                return Err(FrameError::InvalidTerminator);
            }
            *offset = cursor + 2;
            return Ok(Some(value));
        }
        if *byte == b'\n' {
            return Err(FrameError::InvalidTerminator);
        }
        if !byte.is_ascii_digit() {
            return Err(FrameError::InvalidLength);
        }
        value = value
            .checked_mul(10)
            .and_then(|number| number.checked_add(usize::from(*byte - b'0')))
            .ok_or(FrameError::InvalidLength)?;
        cursor += 1;
    }
    Ok(None)
}
