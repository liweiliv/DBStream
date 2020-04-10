#include <assert.h>
#include "qspinlock.h"
#include "mcsSpinlock.h"
#include "threadLocal.h"
#include "util/barrier.h"
#include "yield.h"
static struct mcsSpinlockNode perThreadMcsLockNode[maxThreadCount][MAX_LOCK_LOOP_PER_THREAD];
static uint8_t perThreadMcsLockNodeUseInfo[maxThreadCount];
static inline uint8_t getMcsNodeIndex()
{
	assert(perThreadMcsLockNodeUseInfo[getThreadId()] < MAX_LOCK_LOOP_PER_THREAD);//lock loop over MAX_LOCK_LOOP_PER_THREAD
	return perThreadMcsLockNodeUseInfo[getThreadId()]++;
}
static inline  uint32_t encodeTail(int threadId, int idx)
{
	uint32_t tail;
	tail = (threadId + 1) << _Q_TAIL_CPU_OFFSET;
	tail |= idx << _Q_TAIL_IDX_OFFSET; /* assume < 4 */
	return tail;
}
static inline struct mcsSpinlockNode* decodeTail(uint32_t tail)
{
	int thread = (tail >> _Q_TAIL_CPU_OFFSET) - 1;
	int idx = (tail & _Q_TAIL_IDX_MASK) >> _Q_TAIL_IDX_OFFSET;
	return &perThreadMcsLockNode[thread][idx];
}
DLL_EXPORT void qspinlock::queuedSpinlockSlowpath(uint32_t _val)
{
	struct mcsSpinlockNode* prev, * next, * node;
	uint32_t old, tail;
	int idx;

	/*
	 * Wait for in-progress pending->locked hand-overs with a bounded
	 * number of spins so that we guarantee forward progress.
	 *
	 * 0,1,0 -> 0,0,1
	 */
	if (_val == _Q_PENDING_VAL) {
		while ((_val = *(volatile uint32_t*)(&this->val)) == _Q_PENDING_VAL)
			yield();
	}

	/*
	 * If we observe any contention; queue.
	 */
	if (_val & ~_Q_LOCKED_MASK)
		goto queue;

	/*
	 * trylock || pending
	 *
	 * 0,0,* -> 0,1,* -> 0,0,1 pending, trylock
	 */

	_val = setPendingAndGetOldValue();

	/*
	 * If we observe contention, there is a concurrent locker.
	 *
	 * Undo and queue; our setting of PENDING might have made the
	 * n,0,0 -> 0,0,0 transition fail and it will now be waiting
	 * on @next to become !NULL.
	 */
	if (unlikely(_val & ~_Q_LOCKED_MASK)) {

		/* Undo PENDING if we set it. */
		if (!(_val & _Q_PENDING_MASK))
			unsetPending();

		goto queue;
	}

	/*
	 * We're pending, wait for the owner to go away.
	 *
	 * 0,1,1 -> 0,1,0
	 *
	 * this wait loop must be a load-acquire such that we match the
	 * store-release that clears the locked bit and create lock
	 * sequentiality; this is because not all
	 * clear_pending_set_locked() implementations imply full
	 * barriers.
	 */
	if (_val & _Q_LOCKED_MASK)
	{
		while ((*(volatile uint32_t*)(&this->val)) & _Q_LOCKED_MASK)
			yield();
	}
	/*
	 * take ownership and clear the pending bit.
	 *
	 * 0,1,0 -> 0,0,1
	 */
	clearPendingSetLocked();
	return;

	/*
	 * End of pending bit optimistic spinning and beginning of MCS
	 * queuing.
	 */
queue:
	idx = getMcsNodeIndex();
	tail = encodeTail(getThreadId(), idx);
	node = &perThreadMcsLockNode[getThreadId()][idx];


	node->wait = false;
	node->next = nullptr;
	barrier;
	/*
	 * We touched a (possibly) cold cacheline in the per-cpu queue node;
	 * attempt the trylock once more in the hope someone let go while we
	 * weren't watching.
	 */
	if (try_lock())
		goto release;

	/*
	 * Ensure that the initialisation of @node is complete before we
	 * publish the updated tail via xchg_tail() and potentially link
	 * @node into the waitqueue via WRITE_ONCE(prev->next, node) below.
	 */
	barrier;
	/*
	 * Publish the updated tail.
	 * We have already touched the queueing cacheline; don't bother with
	 * pending stuff.
	 *
	 * p,*,* -> n,*,*
	 */
	old = xchgTail(tail);
	next = nullptr;

	/*
	 * if there was a previous node; link it and wait until reaching the
	 * head of the waitqueue.
	 */
	if (old & _Q_TAIL_MASK) {
		prev = decodeTail(old);

		/* Link @node into the waitqueue. */
		prev->next = node;
		while (!node->wait)
			yield();
		next = (mcsSpinlockNode*)node->next;
	}

	/*
	 * we're at the head of the waitqueue, wait for the owner & pending to
	 * go away.
	 *
	 * *,x,y -> *,0,0
	 *
	 * this wait loop must use a load-acquire such that we match the
	 * store-release that clears the locked bit and create lock
	 * sequentiality; this is because the set_locked() function below
	 * does not imply a full barrier.
	 */
	while ((_val = (*(volatile uint32_t*)&this->val)) & _Q_LOCKED_PENDING_MASK)
		yield();
	/*
	 * claim the lock:
	 *
	 * n,0,0 -> 0,0,1 : lock, uncontended
	 * *,*,0 -> *,*,1 : lock, contended
	 *
	 * If the queue head is the only one in the queue (lock value == tail)
	 * and nobody is pending, clear the tail code and grab the lock.
	 * Otherwise, we only need to grab the lock.
	 */

	 /*
	  * In the PV case we might already have _Q_LOCKED_VAL set, because
	  * of lock stealing; therefore we must also allow:
	  *
	  * n,0,1 -> 0,0,1
	  *
	  * Note: at this point: (val & _Q_PENDING_MASK) == 0, because of the
	  *       above wait condition, therefore any concurrent setting of
	  *       PENDING will make the uncontended transition fail.
	  */
	if ((_val & _Q_TAIL_MASK) == tail)
	{
#if defined OS_WIN
		if(InterlockedCompareExchange(&this->val, _Q_LOCKED_VAL, _val) == _val)
#elif defined OS_LINUX
		if(atomicCmpxchg(&this->aval, val, _Q_LOCKED_VAL) ==_val)
#endif
			goto release; /* No contention */
	}

	/*
	 * Either somebody is queued behind us or _Q_PENDING_VAL got set
	 * which will then detect the remaining tail and queue behind us
	 * ensuring we'll see a @next.
	 */
	*(volatile uint8_t*)&this->locked = _Q_LOCKED_VAL;
	/*
	 * contended path; wait for next if not observed yet, release.
	 */
	if (!next)
	{
		while ((next = (mcsSpinlockNode*)node->next) == nullptr)
			yield();
	}
	next->wait = true;
release:
	perThreadMcsLockNodeUseInfo[getThreadId()]--;
}
