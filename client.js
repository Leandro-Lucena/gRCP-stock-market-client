var PROTO_PATH = __dirname + "/protos/stock_market.proto";

var grpc = require("@grpc/grpc-js");
var protoLoader = require("@grpc/proto-loader");
var readline = require("readline");
var fs = require("fs");
var packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: false,
});
var stock_market_proto =
  grpc.loadPackageDefinition(packageDefinition).stock_market;

var target = "localhost:5126";
var client = new stock_market_proto.StockPrice(
  target,
  grpc.credentials.createInsecure()
);
var deadline = new Date();
deadline.setSeconds(deadline.getSeconds() + 8);

function unary() {
  var request = {
    symbol: "PETR4",
  };
  client.getStockPrice(request, { deadline }, function (err, response) {
    console.log("[Unary] Action price: ", response);
  });
}

function serverStreaming() {
  var call = client.GetStockPriceServerStreaming(request, { deadline });
  call.on("data", function (response) {
    console.log("[Server Streaming] Action price: ", response);
  });
  call.on("error", function (err) {
    console.error("[Server Streaming] Error: ", err);
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
        console.error("[Client Streaming] Error: ", err);
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
    console.error("[Bidirectional Streaming] Error: ", err);
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
