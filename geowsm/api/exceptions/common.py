import fastapi_jsonrpc as jsonrpc


NO_IWORKERS = -32001


class JsonrpcError(jsonrpc.BaseError):
    CODE = -32000
    MESSAGE = "Internal Service Error"

    def __init__(self, code=CODE, message=MESSAGE):
        super().__init__()
        self.CODE = code
        self.MESSAGE = message
