// Emits only iPhone Mirroring audio as 48 kHz mono float32 PCM on stdout.
import Foundation
import ScreenCaptureKit
import CoreMedia
import CoreAudio
import CoreGraphics

func fail(_ message: String) -> Never {
    FileHandle.standardError.write(Data((message + "\n").utf8))
    exit(1)
}

final class AudioOutput: NSObject, SCStreamOutput, SCStreamDelegate, @unchecked Sendable {
    func stream(_ stream: SCStream, didStopWithError error: Error) {
        fail("Mirroring capture stopped: \(error.localizedDescription)")
    }

    func stream(_ stream: SCStream, didOutputSampleBuffer buffer: CMSampleBuffer, of type: SCStreamOutputType) {
        guard type == .audio, buffer.isValid, CMSampleBufferDataIsReady(buffer) else { return }
        guard let description = CMSampleBufferGetFormatDescription(buffer),
              let format = CMAudioFormatDescriptionGetStreamBasicDescription(description)?.pointee,
              format.mFormatID == kAudioFormatLinearPCM,
              format.mFormatFlags & kAudioFormatFlagIsFloat != 0,
              format.mBitsPerChannel == 32,
              format.mSampleRate == 48000,
              format.mChannelsPerFrame == 1,
              let block = CMSampleBufferGetDataBuffer(buffer) else {
            fail("Unexpected capture audio format")
        }
        let count = CMBlockBufferGetDataLength(block)
        guard count > 0 else { return }
        var data = Data(count: count)
        let status = data.withUnsafeMutableBytes { bytes in
            CMBlockBufferCopyDataBytes(block, atOffset: 0, dataLength: count, destination: bytes.baseAddress!)
        }
        guard status == kCMBlockBufferNoErr else { fail("Could not read capture audio") }
        do { try FileHandle.standardOutput.write(contentsOf: data) }
        catch { fail("Capture output closed") }
    }
}

@main
struct MirrorAudio {
    static func main() async {
        guard CGPreflightScreenCaptureAccess() else {
            fail("Allow Screen & System Audio Recording for the terminal/app running this command")
        }
        do {
            let content = try await SCShareableContent.excludingDesktopWindows(true, onScreenWindowsOnly: false)
            guard let app = content.applications.first(where: { $0.bundleIdentifier == "com.apple.ScreenContinuity" }),
                  let display = content.displays.first else {
                fail("Open iPhone Mirroring and play FANS before starting subtitles")
            }
            let filter = SCContentFilter(display: display, including: [app], exceptingWindows: [])
            let config = SCStreamConfiguration()
            config.capturesAudio = true
            config.captureMicrophone = false
            config.excludesCurrentProcessAudio = true
            config.sampleRate = 48000
            config.channelCount = 1
            config.width = 2
            config.height = 2
            config.minimumFrameInterval = CMTime(value: 1, timescale: 1)
            config.queueDepth = 3
            let output = AudioOutput()
            let stream = SCStream(filter: filter, configuration: config, delegate: output)
            try stream.addStreamOutput(output, type: .audio, sampleHandlerQueue: DispatchQueue(label: "fans.audio"))
            try await stream.startCapture()
            FileHandle.standardError.write(Data("Capturing iPhone Mirroring audio\n".utf8))
            while true { try await Task.sleep(nanoseconds: 3_600_000_000_000) }
        } catch { fail(error.localizedDescription) }
    }
}
