use std::num::{NonZeroU32, NonZeroU8};

use mediasoup::prelude::{
    MimeTypeAudio,
    MimeTypeVideo,
    RtcpFeedback,
    RtpCodecCapability,
    RtpCodecParametersParameters,
};

use super::error::CodecError;

/// Returns the list of supported RTP codec capabilities for the SFU
pub fn get_codecs() -> Result<Vec<RtpCodecCapability>, CodecError> {
    Ok(vec![
        RtpCodecCapability::Audio {
            mime_type: MimeTypeAudio::Opus,
            preferred_payload_type: None,
            clock_rate: NonZeroU32::new(48000).ok_or(CodecError::InvalidClockRate)?,
            channels: NonZeroU8::new(2).ok_or(CodecError::InvalidChannels)?,
            parameters: RtpCodecParametersParameters::from([("useinbandfec", 1_u32.into())]),
            rtcp_feedback: vec![RtcpFeedback::TransportCc],
        },
        RtpCodecCapability::Video {
            mime_type: MimeTypeVideo::Vp8,
            preferred_payload_type: None,
            clock_rate: NonZeroU32::new(90000).ok_or(CodecError::InvalidClockRate)?,
            parameters: RtpCodecParametersParameters::default(),
            rtcp_feedback: vec![
                RtcpFeedback::Nack,
                RtcpFeedback::NackPli,
                RtcpFeedback::CcmFir,
                RtcpFeedback::TransportCc,
            ],
        },
        RtpCodecCapability::Video {
            mime_type: MimeTypeVideo::H264,
            preferred_payload_type: None,
            clock_rate: NonZeroU32::new(90000).ok_or(CodecError::InvalidClockRate)?,
            parameters: RtpCodecParametersParameters::from([
                ("level-asymmetry-allowed", 1_u32.into()),
                ("packetization-mode", 1_u32.into()),
                ("profile-level-id", "42e01f".into()),
            ]),
            rtcp_feedback: vec![
                RtcpFeedback::Nack,
                RtcpFeedback::NackPli,
                RtcpFeedback::CcmFir,
                RtcpFeedback::TransportCc,
            ],
        },
    ])
}
