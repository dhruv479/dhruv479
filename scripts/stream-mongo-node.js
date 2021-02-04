const XLSX = require("xlsx");
const mongodb = require("mongodb");
const { Transform } = require("stream");
const csvStringify = require("csv-stringify");
const fs = require("fs");

const outputStream = fs.createWriteStream("output.csv");

const streamHandler = new Transform({
  readableObjectMode: true,
  writableObjectMode: true, // Enables us to use object in chunk
  transform(chunk, encoding, callback) {
    // chunk: { userId: "userA", firstName: "User", lastName: "Name", email: "usera@example.com" }
    /** Mapping Handling row to header */
    const { userId, firstName, lastName, email } = chunk;
    
    const row = { userIdValue: userId, nameValue: `${firstName} ${lastName}`, emailValue: email };
    this.push(row);
    
    callback();
  },
});

async function connectDB() {
  let db = null;
  try {
    db = await mongodb.MongoClient.connect("mongo://localhost:27017");
    console.log("DB CONNECTED");
  } catch (err) {
    console.log(JSON.stringify(err));
  }
  return db;
}

async function main() {
  
  const workbook = XLSX.readFile("input.xlsx");
  const sheetName = workbook.SheetNames[0];
  const inputRows = XLSX.utils.sheet_to_json(
    workbook.Sheets[sheetName]
  );
  const userIdList = inputRows.map(user => user.userId);
  
  const dbInstance = await connectDB();
  const collection = dbInstance.db("dbName").collection("User");
  const cursor = collection.find({"userId": {$in: userIdList}});
  
  const columns = {
    userIdValue: "userId",
    nameValue: "Name",
    emailValue: "Email"
  };
  const stringifier = csvStringify({ header: true, columns: columns });
  let stream = cursor.pipe(streamHandler).pipe(stringifier);
  stream = stream.pipe(outputStream);
  
  // handles flow after the stream is finished
  stream.on("finish", () => {
    dbClient.close();
    console.log("OPERATION SUCCESS 🎉");
    process.exit()
  });
}
