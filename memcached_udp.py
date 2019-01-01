# Copyright 2015 Idan Moyal. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import itertools
import socket
import struct
import threading
import time

from six import advance_iterator


class Client(object):

    class MemcachedServerNotRespondingError(Exception):
        pass

    def __init__(self, servers, debug=False, response_timeout=10):
        self.servers = servers
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(5)
        self._results = {}
        self._response_timeout = response_timeout
        self._debug = debug
        self._request_id_generator = itertools.count()

    def _get_results_handler(self):
        try:
            data, server = self.socket.recvfrom(4096)
            udp_header = struct.unpack('!Hhhh', data[:8])
            if self._debug:
                print(
                    'memcached_udp: results_handler [server]: {0}'.format(
                        server))
                print('memcached_udp: results_handler [data]: {0}'.format(
                    data))
                print('memcached_udp: id={0}, packet_number={1}, '
                      'total_packets={2}, misc={3}'.format(*udp_header))

            request_id = udp_header[0]
            if request_id in self._results:
                return data[8:]
            elif self._debug:
                print('memcached_udp: request id not found in results - '
                      'ignoring... [request_id={0}]'.format(request_id))

        except socket.timeout:
            return None

    @staticmethod
    def _get_udp_header(request_id):
        return struct.pack('!Hhhh', request_id, 0, 1, 0)

    def _get_request_id(self, server):
        request_id = advance_iterator(self._request_id_generator)
        if request_id in self._results:
            raise RuntimeError(
                'Request id already exists for server [server={0}, '
                'request_id={1}]'.format(server, request_id))
        if request_id > 60000:
            self._request_id_generator = itertools.count()
        self._results[request_id] = None
        return request_id

    def set(self, key, value):
        server = self.servers[0]
        request_id = self._get_request_id(server)
        cmd = b''.join([
            self._get_udp_header(request_id),
            b'set ',
            key.encode(),
            b' 0 0 ',
            str(len(value)).encode(),
            b'\r\n',
            value.encode(), b'\r\n',
        ])

        self.socket.sendto(cmd, server)
        r = self._get_results_handler()

        if r.split(b'\r\n')[0] != b'STORED':
            raise RuntimeError(
                'Error storing "{0}" in {1}'.format(key, server))

    def get(self, key):
        server = self.servers[0]
        request_id = self._get_request_id(server)
        cmd = b''.join([
            self._get_udp_header(request_id),
            b'get ',
            key.encode(),
            b'\r\n',
        ])

        self.socket.sendto(cmd, server)
        r = self._get_results_handler()

        if r.startswith(b'VALUE'):
            arr = r.split(b'\r\n')
            return b'\r\n'.join(arr[1:len(arr)-2]).decode()
        return None
