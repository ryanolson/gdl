//! A future built in place: a slot of 256 bytes, aligned to 8, that holds
//! one future and polls and drops it where it was written. A unary call's
//! own future holds a slot, so a local call builds the service's future
//! inside it and makes no allocation of its own. A future that is larger,
//! or aligned to more than 8, is boxed, and the box is held in the slot.
//!
//! The slot is never moved once a future is in it: `place` and `poll` take
//! it pinned, and it is `!Unpin`. A pair of functions made for the
//! future's type polls and drops it, so nothing here reads the layout of a
//! fat pointer. The buffer's address is taken with `addr_of_mut!` from a
//! pointer to the slot, never through a `&mut` to the buffer: the buffer
//! is `Unpin`, so a `&mut` to it would claim its bytes as unique and
//! invalidate a borrow that the future holds into itself across a pending
//! poll. A `&mut` to the slot itself does not, because the slot is
//! `!Unpin`.

#![allow(unsafe_code)]

use std::future::Future;
use std::marker::{PhantomData, PhantomPinned};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll};

/// The largest future, in bytes, that is built in the slot.
pub(super) const ROOM: usize = 256;
/// The largest alignment of a future that is built in the slot.
pub(super) const ALIGN: usize = 8;

/// One future, built in place or boxed, polled where it is.
pub(super) struct Slot<'a, T> {
    buffer: Buffer,
    /// The functions for the future in `buffer`, or `None` when it holds
    /// none.
    held: Option<Held<T>>,
    /// Once a future is in the buffer it must not move.
    _pinned: PhantomPinned,
    /// The auto traits of the boxed future the slot stands for: `Send`,
    /// since `place` takes only a `Send` future, and not `Sync`. The bytes
    /// of the buffer would make it both, whatever it holds.
    _holds: PhantomData<Pin<Box<dyn Future<Output = T> + Send + 'a>>>,
}

/// The bytes a future is built in.
#[repr(C, align(8))]
struct Buffer([MaybeUninit<u8>; ROOM]);

const _: () = assert!(align_of::<Buffer>() == ALIGN && size_of::<Buffer>() == ROOM);

/// The functions made for the type of the future in a slot, each called
/// with the buffer's address.
struct Held<T> {
    poll: unsafe fn(*mut u8, &mut Context<'_>) -> Poll<T>,
    drop: unsafe fn(*mut u8),
}

impl<T> Clone for Held<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Held<T> {}

impl<'a, T> Slot<'a, T> {
    /// A slot that holds no future.
    #[inline]
    pub(super) const fn new() -> Self {
        Self {
            buffer: Buffer([MaybeUninit::uninit(); ROOM]),
            held: None,
            _pinned: PhantomPinned,
            _holds: PhantomData,
        }
    }

    /// Whether a future of type `F` is built in the buffer, rather than
    /// boxed.
    #[inline]
    pub(super) const fn fits<F>() -> bool {
        size_of::<F>() <= ROOM && align_of::<F>() <= ALIGN
    }

    /// Hold `future`, in the buffer when it fits, and in a box held in the
    /// buffer otherwise. A future the slot held before is dropped first.
    #[inline]
    pub(super) fn place<F>(self: Pin<&mut Self>, future: F)
    where
        F: Future<Output = T> + Send + 'a,
    {
        // SAFETY: nothing is moved out of the slot; it is used only
        // through this pointer from here on.
        let this: *mut Self = unsafe { self.get_unchecked_mut() };
        // SAFETY: `this` is the pinned slot, and no other reference to it
        // is live.
        unsafe { Self::clear(this) };
        if Self::fits::<F>() {
            // SAFETY: the slot is empty, and `F` fits.
            unsafe { Self::write(this, future) }
        } else {
            // SAFETY: the slot is empty, and a box is one pointer, which
            // fits.
            unsafe { Self::write(this, Box::pin(future)) }
        }
    }

    /// Poll the future the slot holds. The future is dropped when it is
    /// ready, so that what it holds is released at once.
    ///
    /// # Panics
    ///
    /// When the slot holds no future: it was never given one, or its
    /// future is done, or the call that was to build it panicked.
    #[inline]
    pub(super) fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        // SAFETY: nothing is moved out of the slot.
        let this: *mut Self = unsafe { self.get_unchecked_mut() };
        // SAFETY: `this` is valid; reading the field copies two function
        // pointers and makes no reference to the buffer.
        let Some(held) = (unsafe { (*this).held }) else {
            panic!("a unary call polled after it completed");
        };
        // SAFETY: `held` was made for the type of the future in the
        // buffer, which is live and pinned there.
        match unsafe { (held.poll)(Self::at(this), cx) } {
            Poll::Pending => Poll::Pending,
            Poll::Ready(out) => {
                // SAFETY: `this` is the pinned slot, with no other
                // reference live.
                unsafe { Self::clear(this) };
                Poll::Ready(out)
            }
        }
    }

    /// The buffer's address, as a pointer derived from `this` with no
    /// reference to the buffer in between.
    ///
    /// # Safety
    ///
    /// `this` points to a live slot.
    #[inline(always)]
    unsafe fn at(this: *mut Self) -> *mut u8 {
        // SAFETY: the caller's contract. `addr_of_mut!` makes a pointer to
        // the field without a reference, so it leaves the borrows the
        // future holds into the buffer valid.
        unsafe { ptr::addr_of_mut!((*this).buffer).cast() }
    }

    /// Write `placed` into the empty buffer, with the functions made for
    /// its type.
    ///
    /// # Safety
    ///
    /// `this` points to a live, pinned slot that holds no future, and `P`
    /// fits the buffer.
    #[inline(always)]
    unsafe fn write<P: Future<Output = T>>(this: *mut Self, placed: P) {
        debug_assert!(Self::fits::<P>());
        // SAFETY: the buffer is aligned to `ALIGN` and holds `ROOM` bytes,
        // which `P` fits, and it holds nothing to overwrite.
        unsafe {
            Self::at(this).cast::<P>().write(placed);
            (*this).held = Some(Held {
                poll: poll_at::<P>,
                drop: drop_at::<P>,
            });
        }
    }

    /// Drop the future the slot holds, if any. The slot is marked empty
    /// first, so that a future whose `Drop` panics is not dropped again
    /// when the slot is.
    ///
    /// # Safety
    ///
    /// `this` points to a live slot, with no other reference live.
    #[inline]
    unsafe fn clear(this: *mut Self) {
        // SAFETY: the caller's contract; `take` borrows `held` alone.
        if let Some(held) = unsafe { (*this).held.take() } {
            // SAFETY: `held` was made for the type of the future in the
            // buffer, which is live, and is dropped this once: the slot no
            // longer names it.
            unsafe { (held.drop)(Self::at(this)) }
        }
    }

    /// The buffer's bytes, for a test to tell where a future lives.
    #[cfg(test)]
    fn buffer(&self) -> std::ops::Range<usize> {
        let start = ptr::addr_of!(self.buffer).addr();
        start..start + ROOM
    }
}

impl<T> Drop for Slot<'_, T> {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `self` is the slot, which is dropped once; the `&mut`
        // to the `!Unpin` slot leaves the future's self-borrows valid.
        unsafe { Self::clear(self) }
    }
}

/// Poll the `P` at `at`.
///
/// # Safety
///
/// `at` holds a live `P` that is never moved until it is dropped.
unsafe fn poll_at<P: Future>(at: *mut u8, cx: &mut Context<'_>) -> Poll<P::Output> {
    // SAFETY: the caller's contract: a live `P`, pinned where it is.
    let future = unsafe { Pin::new_unchecked(&mut *at.cast::<P>()) };
    future.poll(cx)
}

/// Drop the `P` at `at`.
///
/// # Safety
///
/// `at` holds a live `P`, which is not used again.
unsafe fn drop_at<P>(at: *mut u8) {
    // SAFETY: the caller's contract.
    unsafe { ptr::drop_in_place(at.cast::<P>()) }
}

#[cfg(test)]
mod tests;
