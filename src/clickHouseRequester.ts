import { QueryParams, createClient } from "@clickhouse/client";
import {
	PlywoodRequester,
	PlywoodLocator,
	basicLocator,
} from "plywood-base-api";
import { Readable } from "readable-stream";

export interface ClickHouseRequesterParameters {
	locator?: PlywoodLocator;
	host?: string;
	user: string;
	password: string;
	database: string;
	rowResultHandler?: (json:any) => any;
}

export function clickHouseRequesterFactory(
	parameters: ClickHouseRequesterParameters
): PlywoodRequester<string> {
	let locator = parameters.locator;
	if (!locator) {
		let host = parameters.host;
		if (!host) throw new Error("must have a `host` or a `locator`");
		locator = basicLocator(host, 8123);
	}
	let user = parameters.user;
	let password = parameters.password;
	let database = parameters.database;
	let rowResultHandler = parameters.rowResultHandler || ((a:any) => a);

	return (request): Readable => {
		let query: QueryParams = {
			query: request.query,
			format: "JSONEachRow",
		};
		let stream = new Readable({
			objectMode: true,
			read: function () {},
		});
		
		locator!()
			.then((location) =>
				createClient({
					host: `http://${location!.hostname}:${location.port || 8123}`,
					username: user,
					password,
					database: database,
					clickhouse_settings: {
						output_format_json_quote_64bit_integers: 0,
						enable_http_compression: 0,
					}
				})
			)
			.then((client) => client.query(query))
			.then((res) => {
				let _stream = res.stream();
				_stream.on("data", (chunk) =>
					chunk.forEach((c: any) => stream.push(rowResultHandler(c.json())))
				);
				_stream.on("end", () => stream.push(null));
				_stream.on("error", (err) => stream.push(err));
			})
			.catch((err: Error) => stream.emit("error", err));

		return stream;
	};
}

export function TZRowResultHandler(){
	var dateTimeRegx = /\d+[/-]\d+[/-]\d+ \d+:\d+:\d+/;
	return function(json:any){
		Object.keys(json).forEach(
		  (k) => dateTimeRegx.test(json[k]) && (json[k] += "Z")
		);
		return json
	}
}