#! /usr/bin/env ts-node

import Path from "path";
import fs from 'fs';

const logName = process.argv[2];
if (!logName)
	throw new Error('log name is required');

const logText =
	fs.readFileSync(Path.relative(Path.dirname(__dirname), `log/${logName}.json`), "utf8");

type Span = any[]
namespace Span {
	export const fromLog = (logObj: any): any[] => {
		if (!logObj.span)
			throw new Error('no span on logOb');

		return [...(logObj.spans || []), logObj.span];
	}
}

type OpenSpans = Map<string, {
	span: any,
	children: OpenSpans,
}>;
namespace OpenSpans {
	export const open = (span: Span, state: OpenSpans) => {
		if (span.length === 0)
			return;

		const [head, ...tail] = span;
		const key = JSON.stringify(head);

		if (!state.has(key)) {
			state.set(key, {
				span: head,
				children: new Map(),
			});
		}

		open(tail, state.get(key)!.children);
	}

	export const close = (span: Span, state: OpenSpans) => {
		if (span.length === 0)
			return;

		const [head, ...tail] = span;
		const key = JSON.stringify(head);
		if (!state.has(key)) {
			throw new Error(`span ${key} not found`);
		}

		if (tail.length === 0) {
			state.delete(key);
		}
		else {
			close(tail, state.get(key)!.children);
		}
	}

	export const display = (state: OpenSpans, indent: number = 0) => {
		for (const [key, value] of state.entries()) {
			console.log(`${'  '.repeat(indent)}${key}`);
			display(value.children, indent + 1);
		}
	}
}

const openSpans = new Map();

for (const logLine of logText.split('\n')) {
	if (!logLine.trim())
		continue;

	const log = JSON.parse(logLine);
	if (!log.span)
		continue;


	const span = Span.fromLog(log);

	if (log.fields?.message === 'new')
		OpenSpans.open(span, openSpans);
	else if (log.fields?.message === 'close') {
		OpenSpans.close(span, openSpans);
	};
}

OpenSpans.display(openSpans);

