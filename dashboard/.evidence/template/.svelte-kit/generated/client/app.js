import * as client_hooks from '../../../src/hooks.client.js';


export { matchers } from './matchers.js';

export const nodes = [
	() => import('./nodes/0'),
	() => import('./nodes/1'),
	() => import('./nodes/2'),
	() => import('./nodes/3'),
	() => import('./nodes/4'),
	() => import('./nodes/5'),
	() => import('./nodes/6'),
	() => import('./nodes/7'),
	() => import('./nodes/8'),
	() => import('./nodes/9'),
	() => import('./nodes/10'),
	() => import('./nodes/11'),
	() => import('./nodes/12')
];

export const server_loads = [3];

export const dictionary = {
		"/": [4],
		"/About me": [5],
		"/Data Pipeline & Architecture": [6],
		"/Executive Sales Intelligence": [7],
		"/GitHub Process": [8],
		"/Governance & Security": [9],
		"/explore/console": [10,[2]],
		"/explore/schema": [11,[2]],
		"/settings": [~12,[3]]
	};

export const hooks = {
	handleError: client_hooks.handleError || (({ error }) => { console.error(error) }),

	reroute: (() => {})
};

export { default as root } from '../root.svelte';