const fs = require('fs'); //the createWriteStream in this appears to be the fastest way i can find to output a large file
const path = require('path');
const csv = require('csv-parser'); // 13322.0864 ms

// different packages for prarsing the CSV file
// const csv = require("csv-parse"); // 18414.917 ms
// const csv = require("fast-csv"); // 31341.5731 ms

// Other packages tried to get better performance
// const batching = require("batching-fs-writestream"); // 12370ms, also has a huge lag (i assume saving the file after)
// const fastJson = require("fast-json-stringify"); //significantly slower than JSON.stringify, 17375ms vs 13296ms

// one of the requirements is speed, so we need to measure what way is faster:
// https://nodejs.org/api/perf_hooks.html#perf_hooks_performance_timing_api
const { PerformanceObserver, performance } = require('perf_hooks');

const obs = new PerformanceObserver((items) => {
	console.log(`File prcessed in ${items.getEntries()[0].duration}ms.`);
	performance.clearMarks();
});
obs.observe({ entryTypes: [ 'measure' ] });

// actual program begins here

let fileName = 'node-data-processing-medium-data.csv';
//let fileName = "test.csv";
let folder = 'data';
let jsonFileName = path.resolve(__dirname, folder, `${path.basename(fileName, '.csv')}.json`);

// start performance testing now
performance.mark('A');

let fileToProcess = path.resolve(__dirname, folder, fileName);
let writeStream = fs.createWriteStream(jsonFileName);

let firstRow = true;
console.log(`Processing ${fileToProcess}`);


fs.access(fileToProcess, fs.F_OK, (err) => {
  if (err) {
    console.error(`File not found: ${fileToProcess}`);
    console.error(`Please make sure file exists`);
    process.exit(1);
  }
});
fs
  .createReadStream(fileToProcess, 'utf8')
  .pipe(csv())
  .on('error', (error) => console.error(error))
  .on('data', (row) => addToPipe(row, writeStream))
  .on('end', (x) => endProcessing(writeStream));


function addToPipe(row, stream) {
  // add new lines to make the file more human readable, not necessary
	if (firstRow) {
		stream.write(`[\r\n${JSON.stringify(row)}`);
		firstRow = false;
	} else {
		stream.write(`,\r\n${JSON.stringify(row)}`); //this is very slightly faster than adding the strings together
	}
}

function endProcessing(stream) {
	stream.write(`\r\n]`);
	stream.end();
	//end performance testing
	performance.mark('B');
	performance.measure('A to B', 'A', 'B');
	console.log(`Processing complete.`);
}
