const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');

let fileName = 'node-data-processing-medium-data.csv';
//let fileName = "test.csv";
let folder = 'data';
let jsonFileName = path.resolve(__dirname, folder, `${path.basename(fileName, '.csv')}.json`);

let fileToProcess = path.resolve(__dirname, folder, fileName);
let writeStream = fs.createWriteStream(jsonFileName);

let firstRow = true;
console.log(`Processing ${fileToProcess}`);

fs
	.createReadStream(fileToProcess, 'utf8')
	.pipe(csv.parse({ headers: true }))
	.on('error', (error) => console.error(error))
	.on('data', (row) => addToPipe(row, writeStream))
	.on('end', (x) => endProcessing(writeStream));

function addToPipe(row, stream) {
	// add new lines to make the file more human readable, not necessary
	if (firstRow) {
		stream.write(`[\r\n${JSON.stringify(row)}`);
		firstRow = false;
	} else {
		stream.write(`,\r\n${JSON.stringify(row)}`);
	}
}

function endProcessing(stream) {
	stream.write(`\r\n]`);
	stream.end();
	console.log(`Processing complete.`);
}
