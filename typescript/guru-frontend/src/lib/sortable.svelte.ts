import { tick } from 'svelte';

/** How long a written order is shown before the stored one takes over regardless. */
const HOLD_MS = 3000;

type Drag = { id: string; pointerId: number; from: string[] };

const sameOrder = (a: string[], b: string[]) =>
	a.length === b.length && a.every((id, index) => b[index] === id);

/**
 * Reordering a keyed list by a handle at the start of each row: press the
 * handle and drag the row past the others, or step it with the arrow keys.
 *
 * A drag moves the row in place — the list shows the order being dragged, and
 * `animate:flip` slides the others out of its way — and a drop hands the new
 * order to `commit`. That order stays on screen until the stored one agrees
 * with it: a write the list is not told the outcome of must not flash the row
 * back to where it was before its snapshot arrives. After HOLD_MS the stored
 * order wins whatever it says.
 *
 * Rows carry `data-sortable-id`, handles `data-sortable-handle`. `list` is the
 * element holding the rows; it must be positioned (`relative`), since the rows'
 * `offsetTop` is where they sit. The pointer is captured by the list, which a
 * drag never moves, so the drag survives its row being moved under it.
 */
export class Sortable {
	list = $state<HTMLElement>();
	/** The row being dragged, drawn lifted. */
	held = $state<string | null>(null);
	/** The order shown instead of the stored one: while dragging, and after a drop until they agree. */
	#shown = $state<string[] | null>(null);
	#drag: Drag | null = null;
	/** Bumped by every write, so an older write's outcome does not undo a newer one. */
	#writes = 0;
	readonly #stored: () => string[];
	readonly #commit: (ids: string[]) => Promise<boolean>;

	constructor(stored: () => string[], commit: (ids: string[]) => Promise<boolean>) {
		this.#stored = stored;
		this.#commit = commit;
		$effect(() => {
			const shown = this.#shown;
			if (shown && this.held === null && sameOrder(this.#stored(), shown)) this.#shown = null;
		});
		$effect(() => () => this.#end());
	}

	/** `rows` in the order to show them; rows the shown order does not know go last, as stored. */
	arrange<T extends { id: string }>(rows: T[]): T[] {
		const shown = this.#shown;
		if (!shown) return rows;
		const rank = new Map(shown.map((id, index) => [id, index]));
		const rankOf = (row: T) => rank.get(row.id) ?? shown.length;
		return [...rows].sort((a, b) => rankOf(a) - rankOf(b));
	}

	/** Forgets the list: another one is shown in its place. */
	reset() {
		this.#end();
		this.#shown = null;
		this.#writes += 1;
	}

	/** `pointerdown` on the handle of row `id`. */
	grab(event: PointerEvent, id: string) {
		const list = this.list;
		if (!list || event.button !== 0 || this.#drag) return;
		try {
			list.setPointerCapture(event.pointerId);
		} catch {
			return;
		}
		// No text selection, no focus: the pointer is dragging.
		event.preventDefault();
		const from = this.#current();
		this.#drag = { id, pointerId: event.pointerId, from };
		this.#shown = from;
		this.held = id;
		list.addEventListener('pointermove', this.#move);
		list.addEventListener('pointerup', this.#drop);
		list.addEventListener('pointercancel', this.#cancel);
		list.addEventListener('lostpointercapture', this.#cancel);
		window.addEventListener('keydown', this.#escape);
	}

	/** `keydown` on the handle of row `id`: the arrow keys move it one row. */
	step(event: KeyboardEvent, id: string) {
		const by = event.key === 'ArrowUp' ? -1 : event.key === 'ArrowDown' ? 1 : 0;
		if (by === 0 || this.#drag) return;
		event.preventDefault();
		const order = this.#current();
		const from = order.indexOf(id);
		const to = from + by;
		if (from < 0 || to < 0 || to >= order.length) return;
		order.splice(from, 1);
		order.splice(to, 0, id);
		void this.#save(order);
		// A row moved in the document can lose focus; its handle keeps it.
		void tick().then(() =>
			this.list
				?.querySelector<HTMLElement>(
					`[data-sortable-id="${CSS.escape(id)}"] [data-sortable-handle]`
				)
				?.focus()
		);
	}

	/** The ids in the order they are shown now. */
	#current(): string[] {
		return this.arrange(this.#stored().map(id => ({ id }))).map(row => row.id);
	}

	#move = (event: PointerEvent) => {
		const drag = this.#drag;
		const list = this.list;
		if (!drag || !list || event.pointerId !== drag.pointerId) return;
		// Where the row goes: after every other row whose middle is above the
		// pointer. `offsetTop` is where a row sits, not where `animate:flip` is
		// sliding it from, so a row under the pointer does not swap back and forth.
		const y = event.clientY - list.getBoundingClientRect().top;
		const others: string[] = [];
		let index = 0;
		for (const row of list.querySelectorAll<HTMLElement>('[data-sortable-id]')) {
			const id = row.dataset.sortableId;
			if (id === undefined || id === drag.id) continue;
			others.push(id);
			if (row.offsetTop + row.offsetHeight / 2 < y) index += 1;
		}
		others.splice(index, 0, drag.id);
		if (!this.#shown || !sameOrder(others, this.#shown)) this.#shown = others;
	};

	#drop = (event: PointerEvent) => {
		const drag = this.#drag;
		if (!drag || event.pointerId !== drag.pointerId) return;
		this.#end();
		const order = this.#shown;
		if (order && !sameOrder(order, drag.from)) void this.#save(order);
		else this.#restore(drag);
	};

	#cancel = (event: PointerEvent) => {
		const drag = this.#drag;
		if (!drag || event.pointerId !== drag.pointerId) return;
		this.#end();
		this.#restore(drag);
	};

	#escape = (event: KeyboardEvent) => {
		const drag = this.#drag;
		if (event.key !== 'Escape' || !drag) return;
		event.preventDefault();
		this.#end();
		this.#restore(drag);
	};

	/** Back to the order before the drag, which is the stored one unless a write is on its way. */
	#restore(drag: Drag) {
		this.#shown = drag.from;
		this.#release();
	}

	#end() {
		this.#drag = null;
		this.held = null;
		const list = this.list;
		list?.removeEventListener('pointermove', this.#move);
		list?.removeEventListener('pointerup', this.#drop);
		list?.removeEventListener('pointercancel', this.#cancel);
		list?.removeEventListener('lostpointercapture', this.#cancel);
		window.removeEventListener('keydown', this.#escape);
	}

	async #save(order: string[]) {
		this.#writes += 1;
		const write = this.#writes;
		this.#shown = order;
		const written = await this.#commit(order);
		if (write !== this.#writes) return;
		if (written) this.#release();
		else this.#shown = null;
	}

	/** The stored order takes over after HOLD_MS, unless something newer holds the list. */
	#release() {
		const write = this.#writes;
		setTimeout(() => {
			if (write === this.#writes && !this.#drag) this.#shown = null;
		}, HOLD_MS);
	}
}
