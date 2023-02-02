const { expect } = require("chai");
const toArray = require("stream-to-array");

const { clickHouseRequesterFactory } = require("../build/clickHouseRequester");

const info = require("./info");

let clickHouseRequester = clickHouseRequesterFactory({
	host: info.clickHouseHost,
	database: info.clickHouseDatabase,
	user: info.clickHouseUser,
	password: info.clickHousePassword,
});

describe("ClickHouse requester", function () {
	this.timeout(5 * 1000);

	describe("error", function () {
		it("throws if there is not host or locator", function () {
			expect(() => {
				clickHouseRequesterFactory({});
			}).to.throw("must have a `host` or a `locator`");
		});

		it("correct error for bad table", (testComplete) => {
			let stream = clickHouseRequester({
				query: "SELECT * FROM not_a_real_datasource",
			});

			stream.on("error", (err) => {
				expect(err.message).to.contain("not_a_real_datasource doesn't exist");
				testComplete();
			});
		});

		// it("correct error ER_PARSE_ERROR", (testComplete) => {
		//   let stream = clickHouseRequester({
		//     query: 'SELECT `channel` AS "Channel", sum(`added` AS "TotalAdded" FROM `wikipedia` WHERE `cityName` = "Tokyo" GROUP BY `channel`;'
		//   });

		//   stream.on('error', (err) => {
		//     expect(err.message).to.contain("ER_PARSE_ERROR");
		//     testComplete();
		//   });
		// });
	});

	describe("basic working", function () {
		it("runs a DESCRIBE", () => {
			let stream = clickHouseRequester({
				query: "DESCRIBE trips;",
			});

			return toArray(stream).then((res) => {
				expect(
					res.map((r) => {
						return r.name + " ~ " + r.type;
					})
				).to.deep.equal([
					"trip_id ~ UInt32",
					"vendor_id ~ Enum8('1' = 1, '2' = 2, '3' = 3, '4' = 4, 'CMT' = 5, 'VTS' = 6, 'DDS' = 7, 'B02512' = 10, 'B02598' = 11, 'B02617' = 12, 'B02682' = 13, 'B02764' = 14, '' = 15)",
					"pickup_date ~ Date",
					"pickup_datetime ~ DateTime",
					"dropoff_date ~ Date",
					"dropoff_datetime ~ DateTime",
					"store_and_fwd_flag ~ UInt8",
					"rate_code_id ~ UInt8",
					"pickup_longitude ~ Float64",
					"pickup_latitude ~ Float64",
					"dropoff_longitude ~ Float64",
					"dropoff_latitude ~ Float64",
					"passenger_count ~ UInt8",
					"trip_distance ~ Float64",
					"fare_amount ~ Float32",
					"extra ~ Float32",
					"mta_tax ~ Float32",
					"tip_amount ~ Float32",
					"tolls_amount ~ Float32",
					"ehail_fee ~ Float32",
					"improvement_surcharge ~ Float32",
					"total_amount ~ Float32",
					"payment_type ~ Enum8('UNK' = 0, 'CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4)",
					"trip_type ~ UInt8",
					"pickup ~ FixedString(25)",
					"dropoff ~ FixedString(25)",
					"cab_type ~ Enum8('yellow' = 1, 'green' = 2, 'uber' = 3)",
					"pickup_nyct2010_gid ~ Int8",
					"pickup_ctlabel ~ Float32",
					"pickup_borocode ~ Int8",
					"pickup_ct2010 ~ String",
					"pickup_boroct2010 ~ String",
					"pickup_cdeligibil ~ String",
					"pickup_ntacode ~ FixedString(4)",
					"pickup_ntaname ~ String",
					"pickup_puma ~ UInt16",
					"dropoff_nyct2010_gid ~ UInt8",
					"dropoff_ctlabel ~ Float32",
					"dropoff_borocode ~ UInt8",
					"dropoff_ct2010 ~ String",
					"dropoff_boroct2010 ~ String",
					"dropoff_cdeligibil ~ String",
					"dropoff_ntacode ~ FixedString(4)",
					"dropoff_ntaname ~ String",
					"dropoff_puma ~ UInt16",
				]);
			});
		});

		// it("runs a SELECT / GROUP BY", () => {
		//   let stream = clickHouseRequester({
		//     query: 'SELECT `channel` AS "Channel", sum(`added`) AS "TotalAdded", sum(`deleted`) AS "TotalDeleted" FROM `wikipedia` WHERE `cityName` = "Tokyo" GROUP BY `channel`;'
		//   });

		//   return toArray(stream)
		//     .then((res) => {
		//       expect(res).to.deep.equal([
		//         {
		//           "Channel": "de",
		//           "TotalAdded": 0,
		//           "TotalDeleted": 109
		//         },
		//         {
		//           "Channel": "en",
		//           "TotalAdded": 3500,
		//           "TotalDeleted": 447
		//         },
		//         {
		//           "Channel": "fr",
		//           "TotalAdded": 0,
		//           "TotalDeleted": 0
		//         },
		//         {
		//           "Channel": "ja",
		//           "TotalAdded": 75168,
		//           "TotalDeleted": 2462
		//         },
		//         {
		//           "Channel": "ko",
		//           "TotalAdded": 0,
		//           "TotalDeleted": 57
		//         },
		//         {
		//           "Channel": "ru",
		//           "TotalAdded": 898,
		//           "TotalDeleted": 194
		//         },
		//         {
		//           "Channel": "zh",
		//           "TotalAdded": 72,
		//           "TotalDeleted": 21
		//         }
		//       ]);
		//     });
		// });

		// it("works correctly with time", () => {
		//   let stream = clickHouseRequester({
		//     query: 'SELECT MAX(`__time`) AS "MaxTime" FROM `wikipedia` GROUP BY ""'
		//   });

		//   return toArray(stream)
		//     .then((res) => {
		//       expect(res).to.deep.equal([
		//         {
		//           "MaxTime": new Date('2015-09-12T23:59:00.000Z')
		//         }
		//       ]);
		//     });
		// })
	});
});
