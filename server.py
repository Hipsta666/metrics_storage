import asyncio as io


def run_server(host, port):
    loop = io.get_event_loop()
    try:
        coro = loop.create_server(ClientServerProtocol, host, port)
        server = loop.run_until_complete(coro)
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
    except:
        raise Error


class Error(Exception):
    def __init__(self, fault="Error"):
        super().__init__()
        self.fault = fault



class ClientServerProtocol(io.Protocol):
    info = {}

    def __init__(self):
        self.check = []
        super().__init__()

    def process_data(self, info):
        method_params = info.split("\n")
        safe = []
        answers = {}
        string = ''
        pr = "ok\n"

        for item in method_params:

            if item != '':
                try:
                    a = str(item).split(' ')
                    b = [a[0], ' '.join(a[1:])]
                    if b[0] == "put":
                        key, value, timestamp = b[1].split()
                        safe.append((b[0], key, float(value), int(timestamp)))
                    elif b[0] == "get":
                        key = b[1]
                        safe.append((b[0], key))
                    else:
                        raise Error
                except:
                    raise Error

        for item in safe:
            if item[0] == 'get' and item[1] == '':
                raise Error

            elif item[0] == "put":
                if item[1] not in self.info:
                    self.info[item[1]] = {}
                self.info[item[1]][item[3]] = item[2]

            elif item[0] == "get":
                info = self.info
                rev = {item[1]: {}}

                if item[1] != "*":
                    if item[1] not in info:
                        info = rev
                    else:
                        info = {item[1]: info[item[1]]}
                res = {}
                for key, timestamp_data in info.items():
                    res[key] = sorted(timestamp_data.items())
                answers = res

            else:
                raise Error
        for key, values in answers.items():
            for timestamp, value in values:
                string += str(key) + ' ' + str(value) + ' ' + str(timestamp) + '\n'
        if string:
            pr += string + "\n"
        return pr + "\n"

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, info):
        try:
            come_q = self.process_data(info.decode())
        except Error as error:
            self.transport.write(f"error\n{error}\n\n".encode())
            return
        self.transport.write(come_q.encode())


if __name__ == "__main__":
    run_server('127.0.0.1', 8888)