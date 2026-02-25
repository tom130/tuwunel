use std::{collections::HashSet as Set, iter::once, ops::Deref};

use futures::{
	Future, Stream, StreamExt,
	stream::{FuturesUnordered, unfold},
};
use ruma::OwnedEventId;
use tuwunel_core::{
	Result, implement,
	matrix::{Event, pdu::AuthEvents},
	smallvec::SmallVec,
	utils::stream::IterStream,
};

#[derive(Default)]
struct Global<Fut: Future + Send> {
	subgraph: Set<OwnedEventId>,
	seen: Set<OwnedEventId>,
	todo: Todo<Fut>,
}

#[derive(Default, Debug)]
struct Local {
	path: Path,
	stack: Stack,
}

type Todo<Fut> = FuturesUnordered<Fut>;
type Path = SmallVec<[OwnedEventId; PATH_INLINE]>;
type Stack = SmallVec<[Frame; STACK_INLINE]>;
type Frame = AuthEvents;

const PATH_INLINE: usize = 16;
const STACK_INLINE: usize = 16;

#[tracing::instrument(
	name = "subgraph_dfs",
	level = "debug",
	skip_all,
	fields(
		starting_events = %conflicted_set.len(),
	)
)]
pub(super) fn conflicted_subgraph_dfs<Fetch, Fut, Pdu>(
	conflicted_set: &Vec<&OwnedEventId>,
	fetch: &Fetch,
) -> impl Stream<Item = OwnedEventId> + Send
where
	Fetch: Fn(OwnedEventId) -> Fut + Sync,
	Fut: Future<Output = Result<Pdu>> + Send,
	Pdu: Event,
{
	let state = Global {
		subgraph: Default::default(),
		seen: Default::default(),
		todo: conflicted_set
			.iter()
			.map(Deref::deref)
			.cloned()
			.map(Local::new)
			.filter_map(Local::pop)
			.map(|(local, event_id)| local.push(fetch, Some(event_id)))
			.collect(),
	};

	unfold(state, |mut state| async {
		let outputs = state
			.todo
			.next()
			.await?
			.pop()
			.map(|(local, event_id)| local.eval(&mut state, conflicted_set, event_id))
			.map(|(local, next_id, outputs)| {
				if !local.stack.is_empty() {
					state.todo.push(local.push(fetch, next_id));
				}

				outputs
			})
			.into_iter()
			.flatten()
			.stream();

		Some((outputs, state))
	})
	.flatten()
}

#[implement(Local)]
#[tracing::instrument(
	name = "descent",
	level = "trace",
	skip_all,
	fields(
		?event_id,
		path = self.path.len(),
		stack = self.stack.len(),
		subgraph = state.subgraph.len(),
		seen = state.seen.len(),
	)
)]
fn eval<Fut: Future + Send>(
	mut self,
	state: &mut Global<Fut>,
	conflicted_event_ids: &Vec<&OwnedEventId>,
	event_id: OwnedEventId,
) -> (Self, Option<OwnedEventId>, Path) {
	let Global { subgraph, seen, .. } = state;

	let insert_path = |global: &mut Global<Fut>, local: &Local| {
		local
			.path
			.iter()
			.filter(|&event_id| global.subgraph.insert(event_id.clone()))
			.cloned()
			.collect()
	};

	if subgraph.contains(&event_id) {
		if self.path.len() <= 1 {
			self.path.pop();
			return (self, None, Path::new());
		}

		let path = insert_path(state, &self);
		self.path.pop();
		return (self, None, path);
	}

	if !seen.insert(event_id.clone()) {
		return (self, None, Path::new());
	}

	if self.path.len() > 1
		&& conflicted_event_ids
			.binary_search(&&event_id)
			.is_ok()
	{
		let path = insert_path(state, &self);
		return (self, Some(event_id), path);
	}

	(self, Some(event_id), Path::new())
}

#[implement(Local)]
async fn push<Fetch, Fut, Pdu>(mut self, fetch: &Fetch, event_id: Option<OwnedEventId>) -> Self
where
	Fetch: Fn(OwnedEventId) -> Fut + Sync,
	Fut: Future<Output = Result<Pdu>> + Send,
	Pdu: Event,
{
	if let Some(event_id) = event_id
		&& let Ok(event) = fetch(event_id).await
	{
		self.stack
			.push(event.auth_events_into().into_iter().collect());
	}

	self
}

#[implement(Local)]
fn pop(mut self) -> Option<(Self, OwnedEventId)> {
	while self.stack.last().is_some_and(Frame::is_empty) {
		self.stack.pop();
		self.path.pop();
	}

	self.stack
		.last_mut()
		.and_then(Frame::pop)
		.inspect(|event_id| self.path.push(event_id.clone()))
		.map(move |event_id| (self, event_id))
}

#[implement(Local)]
fn new(conflicted_event_id: OwnedEventId) -> Self {
	Self {
		path: once(conflicted_event_id.clone()).collect(),
		stack: once(once(conflicted_event_id).collect()).collect(),
	}
}
