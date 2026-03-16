import zmq

from synapse.api.datatype_pb2 import BroadbandFrame
from synapse.api.tap_pb2 import TapConnection, TapType
from synapse.server.nodes.spectral_filter import SpectralFilter as ServerSpectralFilter
from synapse.utils.types import ElectricalBroadbandData


class SpectralFilter(ServerSpectralFilter):
    def __init__(self, id):
        super().__init__(id)
        self.zmq_context = None
        self.zmq_socket = None
        self.seq_number = 0
        self.iface_ip = None
        self.port = None

    async def run(self):
        if not self.zmq_context:
            if not self.iface_ip:
                self.logger.error("iface_ip not configured")
                return

            self.zmq_context = zmq.Context()
            self.zmq_socket = self.zmq_context.socket(zmq.PUB)
            self.port = self.zmq_socket.bind_to_random_port(f"tcp://{self.iface_ip}")

        while self.running:
            data = await self.data_queue.get()
            try:
                filtered_samples = self.apply_filter(data.samples)
            except Exception as e:
                self.logger.error(f"Error filtering data: {e}")
                continue

            filtered_data = ElectricalBroadbandData(
                data.t0, data.bit_width, filtered_samples, data.sample_rate
            )
            await self.emit_data(filtered_data)

            n_samples = len(filtered_samples[0][1])
            for i in range(n_samples):
                frame = BroadbandFrame(
                    timestamp_ns=data.t0 + int(i * 1e9 / data.sample_rate),
                    sequence_number=self.seq_number,
                    frame_data=[int(chan_samples[i]) for _, chan_samples in filtered_samples],
                    sample_rate_hz=data.sample_rate,
                )
                try:
                    self.zmq_socket.send(frame.SerializeToString())
                    self.seq_number += 1
                except Exception as e:
                    self.logger.error(f"Error sending data: {e}")

    def stop(self):
        if self.zmq_socket:
            self.zmq_socket.close()
            self.zmq_socket = None

        if self.zmq_context:
            self.zmq_context.destroy()
            self.zmq_context = None

        return super().stop()

    def configure_iface_ip(self, iface_ip):
        self.iface_ip = iface_ip

    def tap_connections(self):
        return [
            TapConnection(
                name="spectral_filter_sim",
                endpoint=f"tcp://{self.iface_ip}:{self.port}",
                message_type="synapse.BroadbandFrame",
                tap_type=TapType.TAP_TYPE_PRODUCER,
            )
        ]
