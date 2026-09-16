import { useSvelteFlow } from '@xyflow/svelte';

export const ZOOM_MIN = 0.2;
export const ZOOM_MAX = 2;
const ZOOM_STEP = 1.2;

/**
 * The View menu's half of the canvas, which stands in for Svelte Flow's own
 * `Controls`: the lock, the fullscreen toggle, and a zoom the menu can read.
 * Kept out of the flow component because none of it touches the graph — it is
 * the pane, not what is drawn on it.
 */
export type CanvasViewport = {
	/** The lock: dragging, connecting and selecting at once. */
	interactive: boolean;
	fullscreen: boolean;
	/** What the View menu prints; the flow's `onmove` keeps it current. */
	zoom: number;
	/** Centre of the visible pane, in flow coordinates: where new nodes land. */
	position(): { x: number; y: number };
	rescale(step: 'in' | 'out' | 'fit'): Promise<void>;
	toggleFullscreen(): void;
	/** `fitView` and the node panel move the viewport without a move event. */
	syncZoom(): void;
};

/** `pane` is the element the flow fills; it is bound after this is built. */
export function canvasViewport(pane: () => HTMLElement | null): CanvasViewport {
	const { screenToFlowPosition, getViewport, setViewport, getZoom, fitView } = useSvelteFlow();

	let interactive = $state(true);
	let fullscreen = $state(false);
	let zoom = $state(1);

	return {
		get interactive() {
			return interactive;
		},
		set interactive(value) {
			interactive = value;
		},
		get fullscreen() {
			return fullscreen;
		},
		set fullscreen(value) {
			fullscreen = value;
		},
		get zoom() {
			return zoom;
		},
		set zoom(value) {
			zoom = value;
		},

		position() {
			const rect = pane()?.getBoundingClientRect();
			if (!rect) return { x: 0, y: 0 };
			const point = screenToFlowPosition({
				x: rect.left + rect.width / 2,
				y: rect.top + rect.height / 2
			});
			return { x: Math.round(point.x), y: Math.round(point.y) };
		},

		/**
		 * Steps the zoom around the centre of the pane, or fits the whole canvas.
		 * The viewport is moved with `setViewport` rather than `zoomIn`/`zoomOut`,
		 * whose `scaleBy` leaves the transform untouched in Svelte Flow 1.6.
		 */
		async rescale(step) {
			if (step === 'fit') {
				await fitView();
			} else {
				const current = getViewport();
				const wanted = current.zoom * (step === 'in' ? ZOOM_STEP : 1 / ZOOM_STEP);
				const next = Math.min(ZOOM_MAX, Math.max(ZOOM_MIN, wanted));
				const rect = pane()?.getBoundingClientRect();
				// The pane centre stays put: it is the point the zoom is anchored on.
				const cx = (rect?.width ?? 0) / 2;
				const cy = (rect?.height ?? 0) / 2;
				const ratio = next / current.zoom;
				await setViewport({
					x: cx - (cx - current.x) * ratio,
					y: cy - (cy - current.y) * ratio,
					zoom: next
				});
			}
			zoom = getZoom();
		},

		/**
		 * Fullscreens the document, not the flow pane: the menus and dialogs portal
		 * into `document.body`, so anything smaller would render them outside the
		 * fullscreen element and make them invisible.
		 */
		toggleFullscreen() {
			const request = document.fullscreenElement
				? document.exitFullscreen()
				: document.documentElement.requestFullscreen();
			// A refused request is the browser's call, not a failure worth reporting.
			request.catch(() => undefined);
		},

		syncZoom() {
			zoom = getZoom();
		}
	};
}
