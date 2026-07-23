import { useEffect, useRef } from "react";

function VideoPlayer({
  videoId,
  stream,
}: {
  videoId: string;
  stream: MediaStream | null;
}) {
  const videoRef = useRef<HTMLVideoElement>(null);

  useEffect(() => {
    if (!videoRef.current) {
      return;
    }
    videoRef.current.srcObject = stream;

    let callbackHandle: number;

    const callbackFrame = (
      _now: DOMHighResTimeStamp,
      metadata: VideoFrameCallbackMetadata,
    ) => {
      console.log(metadata.rtpTimestamp);
      if (videoRef.current) {
        //callbackHandle = videoRef.current.requestVideoFrameCallback(callbackFrame);
      }
    };

    callbackHandle = videoRef.current.requestVideoFrameCallback(callbackFrame);

    return () => {
      if (videoRef.current && callbackHandle) {
        videoRef.current.cancelVideoFrameCallback(callbackHandle);
      }
    };
  }, [stream]);

  return <video ref={videoRef} autoPlay muted playsInline />;
}

export default VideoPlayer;
