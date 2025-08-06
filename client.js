var PROTO_PATH = __dirname + "/protos/stock_market.proto";

var grpc = require("@grpc/grpc-js");
var protoLoader = require("@grpc/proto-loader");
const grpcStatus = require("grpc-error-status");
var readline = require("readline");
var fs = require("fs");
var packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: false,
});
var serviceConfig = fs.readFileSync("service_config.json", "utf8");
var stock_market_proto =
  grpc.loadPackageDefinition(packageDefinition).stock_market;

function authInterceptor(options, nextCall) {
  var requester = new grpc.RequesterBuilder()
    .withStart((metadata, listener, next) => {
      metadata.set("authorization", "jwt-token");
      return next(metadata, listener);
    })
    .build();
  return new grpc.InterceptingCall(nextCall(options), requester);
}

function loggingInterceptor(options, nextCall) {
  var requester = new grpc.RequesterBuilder()
    .withStart((metadata, listener, next) => {
      listener = {
        onReceiveMessage: function (message, next) {
          console.log(`Received message: ${JSON.stringify(message)}`);
          next(message);
        },
        onReceiveMetadata: function (metadata, next) {
          console.log(`Received metadata: ${JSON.stringify(metadata)}`);
          next(metadata);
        },
        onReceiveStatus: function (status, next) {
          console.log(`Received status: ${JSON.stringify(status)}`);
          next(status);
        },
      };
      console.log(`Client metadata: ${JSON.stringify(metadata)}`);
      next(metadata, listener);
    })
    .withSendMessage((message, next) => {
      console.log(`Sending message: ${JSON.stringify(message)}`);
      next(message);
    })
    .build();
  return new grpc.InterceptingCall(nextCall(options), requester);
}

var target = "localhost:5126";
var client = new stock_market_proto.StockPrice(
  target,
  grpc.credentials.createInsecure(),
  {
    "grpc.service_config": serviceConfig,
    interceptors: [authInterceptor, loggingInterceptor],
  }
);
var deadline = new Date();
deadline.setSeconds(deadline.getSeconds() + 20);

var request = {
  symbol: "PETR4",
};

function handleError(err) {
  if (err.metadata.internalRepr.has("grpc-status-details-bin")) {
    var errorDetails = grpcStatus.parse(err).toObject();
    console.error("Error occurred:", JSON.stringify(errorDetails));
  } else {
    console.error("Error occurred:", err.message);
  }
}

function unary() {
  client.getStockPrice(request, { deadline }, function (err, response) {
    if (err) {
      handleError(err);
      return;
    }
    console.log("[Unary] Action price: ", response);
  });
}

function serverStreaming() {
  var call = client.GetStockPriceServerStreaming(request, { deadline });
  call.on("data", function (response) {
    console.log("[Server Streaming] Action price: ", response);
  });
  call.on("error", function (err) {
    handleError(err);
  });
  call.on("status", function (status) {
    console.log("[Server Streaming] Call ended with status: ", status);
  });
  call.on("end", function () {
    console.log("[Server Streaming] Call ended.");
  });
}

function clientStreaming() {
  var call = client.updateStockPriceClientStreaming(
    { deadline },
    function (err, response) {
      if (err) {
        handleError(err);
        return;
      } else {
        console.log("[Client Streaming] Response: ", response);
      }
    }
  );

  var fileStream = fs.createReadStream("./data/stockprices.txt");
  var rl = readline.createInterface({ input: fileStream });

  rl.on("line", function (line) {
    message = JSON.parse(line);
    call.write(message);
  });

  rl.on("close", function () {
    call.end(
      console.log("[Client Streaming] File read complete, ending call.")
    );
  });
}

function bidirectionalStreaming() {
  var call = client.getStockPriceBidirectionalStreaming();
  call.on("data", function (response) {
    console.log("[Bidirectional Streaming] Action price: ", response);
  });
  call.on("error", function (err) {
    handleError(err);
  });
  call.on("status", function (status) {
    console.log("[Bidirectional Streaming] Call ended with status: ", status);
  });
  call.on("end", function () {
    console.log("[Bidirectional Streaming] Call ended.");
  });

  var rl = readline.createInterface({
    input: process.stdin,
    prompt: "Enter stock symbol or 'exit' to quit: ",
    output: process.stdout,
  });

  rl.on("line", function (line) {
    if (line.trim().toLowerCase() === "exit") {
      rl.close();
      return;
    }
    message = JSON.parse(line);
    call.write(message);
  });

  rl.on("close", function () {
    call.end(console.log("[Client Streaming] Proccess complete, ending call."));
  });
}

// unary();
// serverStreaming();
// clientStreaming();
bidirectionalStreaming();
