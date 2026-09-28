//! Which pieces a torrent tells peers about.
//!
//! A piece is **announced** -- in the bitfield a peer is sent at its handshake, and
//! by a Have when it completes -- when we have it *and* it is advertised. It is served
//! on the same terms: a request for a piece we have not advertised is dropped. See
//! [`crate::ManagedTorrent::set_pieces_advertised`] for the rules, and
//! [`crate::SessionOptions::explicit_piece_advertising`] for the two defaults.
//!
//! The set lives on [`super::ManagedTorrentShared`], which outlives every state a
//! torrent goes through, and not in the chunk tracker, which a restart out of an error
//! builds afresh. So what a torrent was told to advertise is still advertised after
//! that restart, and after the full check it runs -- a piece stays announced for as long
//! as we have it -- and a torrent under explicit advertising announces nothing from the
//! first moment it exists: the set is made with the torrent, before its first check.
//!
//! Its lock is a leaf: nothing is taken while it is held. It is taken under the
//! torrent's state lock wherever the answer has to agree with the have-set (the
//! handshake bitfield, the refusal of a withdrawal), never the other way round.

use std::borrow::Cow;

use librqbit_core::lengths::{Lengths, ValidPieceIndex};

use crate::type_aliases::BF;

/// Why [`crate::ManagedTorrent::set_pieces_advertised`] refused to stop advertising
/// pieces: some of them are announced, and a live torrent never takes an announcement
/// back. Nothing was changed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "refusing to withdraw {} announced piece(s) (first: {:?}) from a live torrent: \
     an announcement ends only with the torrent leaving the swarm",
    pieces.len(),
    pieces.first()
)]
pub struct WithdrawRefused {
    /// The pieces the call would have withdrawn: advertised, and ours.
    pub pieces: Vec<u32>,
}

/// The advertised set of one torrent.
pub(crate) struct AdvertisedPieces {
    /// `None` is every piece: upstream's behaviour, where what we announce is the
    /// have-set itself, byte for byte, with not even an allocation between them. It
    /// becomes a set the first time something is held back.
    set: parking_lot::RwLock<Option<BF>>,
    /// How many bytes a bitfield of this torrent is, for materialising the set.
    bytes: usize,
    /// Made under [`crate::SessionOptions::explicit_piece_advertising`]: the set
    /// started empty, and a live torrent refuses to drop a piece it announced.
    explicit: bool,
}

impl AdvertisedPieces {
    pub(crate) fn new(explicit: bool, lengths: &Lengths) -> Self {
        let bytes = lengths.piece_bitfield_bytes();
        Self {
            set: parking_lot::RwLock::new(explicit.then(|| empty(bytes))),
            bytes,
            explicit,
        }
    }

    /// Whether this torrent was made under explicit advertising.
    pub(crate) fn is_explicit(&self) -> bool {
        self.explicit
    }

    /// Whether every piece is advertised, as upstream: the cheap question a hot path
    /// asks before it takes any lock of the torrent's.
    pub(crate) fn is_everything(&self) -> bool {
        self.set.read().is_none()
    }

    /// Whether `id` is advertised: announced if we have it, served if asked for.
    pub(crate) fn contains(&self, id: ValidPieceIndex) -> bool {
        self.set
            .read()
            .as_ref()
            .is_none_or(|set| set.get(id.get() as usize).is_some_and(|b| *b))
    }

    /// The bitfield we announce, from the have-bitfield's bytes: the pieces we have and
    /// advertise. Borrowed -- `have` itself -- while every piece is advertised, so a
    /// torrent that never touched the set sends exactly upstream's bytes and allocates
    /// nothing on the handshake path.
    ///
    /// It can come out all zeroes. That is a legal bitfield -- what a peer with nothing
    /// sends -- and the truthful answer: we are announcing nothing. The spare bits past
    /// the last piece stay zero, as BEP-3 requires: masking can only clear bits.
    pub(crate) fn announced<'a>(&self, have: &'a [u8]) -> Cow<'a, [u8]> {
        match self.set.read().as_ref() {
            None => Cow::Borrowed(have),
            Some(set) => Cow::Owned(
                have.iter()
                    .zip(set.as_raw_slice().iter().chain(std::iter::repeat(&0)))
                    .map(|(have, advertised)| have & advertised)
                    .collect(),
            ),
        }
    }

    /// The advertised pieces, ascending, or `None` for every piece.
    pub(crate) fn snapshot(&self) -> Option<Vec<u32>> {
        self.set.read().as_ref().map(|set| {
            set.iter_ones()
                .filter_map(|id| u32::try_from(id).ok())
                .collect()
        })
    }

    /// Advertise `ids`. Returns the ones that were not advertised before, in order.
    pub(crate) fn add(
        &self,
        ids: impl IntoIterator<Item = ValidPieceIndex>,
    ) -> Vec<ValidPieceIndex> {
        let mut g = self.set.write();
        let Some(set) = g.as_mut() else {
            return Vec::new();
        };
        ids.into_iter()
            .filter(|id| !set.replace(id.get() as usize, true))
            .collect()
    }

    /// Stop advertising `ids`. Returns how many changed. The caller has established that
    /// none of them is announced to a peer that could still ask for it: see
    /// [`crate::ManagedTorrent::set_pieces_advertised`].
    pub(crate) fn remove(&self, ids: impl IntoIterator<Item = ValidPieceIndex>) -> usize {
        let mut g = self.set.write();
        let bytes = self.bytes;
        let set = g.get_or_insert_with(|| {
            let mut all = empty(bytes);
            all.fill(true);
            all
        });
        ids.into_iter()
            .filter(|id| set.replace(id.get() as usize, false))
            .count()
    }
}

fn empty(bytes: usize) -> BF {
    BF::from_boxed_slice(vec![0u8; bytes].into_boxed_slice())
}

#[cfg(test)]
mod tests {
    use librqbit_core::constants::CHUNK_SIZE;

    use super::*;

    // 12 pieces, so a bitfield spans more than one byte and the trailing bits of the
    // last byte are spare.
    fn lengths() -> Lengths {
        Lengths::new(u64::from(CHUNK_SIZE) * 12, CHUNK_SIZE).unwrap()
    }

    fn ids(l: &Lengths, r: std::ops::Range<u32>) -> Vec<ValidPieceIndex> {
        r.map(|i| l.validate_piece_index(i).unwrap()).collect()
    }

    fn ones(bytes: &[u8]) -> Vec<usize> {
        let bf = BF::from_boxed_slice(bytes.to_vec().into_boxed_slice());
        bf.iter_ones().collect()
    }

    // Every piece we have, in two bytes: 0..12 set, the four spare bits clear.
    const HAVE_ALL: [u8; 2] = [0xff, 0xf0];

    #[test]
    fn the_default_announces_the_have_set_itself() {
        let l = lengths();
        let a = AdvertisedPieces::new(false, &l);
        assert!(a.is_everything());
        assert!(!a.is_explicit());
        assert_eq!(a.snapshot(), None);
        let have = [0b1010_0000, 0x10];
        assert!(matches!(a.announced(&have), Cow::Borrowed(b) if b == have));
        for id in ids(&l, 0..12) {
            assert!(a.contains(id), "piece {id}");
        }
        // Nothing to add to everything.
        assert!(a.add(ids(&l, 0..12)).is_empty());
        assert!(a.is_everything());
    }

    #[test]
    fn explicit_advertising_starts_with_nothing() {
        let l = lengths();
        let a = AdvertisedPieces::new(true, &l);
        assert!(a.is_explicit());
        assert!(!a.is_everything());
        assert_eq!(a.announced(&HAVE_ALL).as_ref(), &[0, 0]);
        for id in ids(&l, 0..12) {
            assert!(!a.contains(id), "piece {id}");
        }
    }

    #[test]
    fn an_advertised_piece_is_announced_only_once_we_have_it() {
        let l = lengths();
        let a = AdvertisedPieces::new(true, &l);
        assert_eq!(a.add(ids(&l, 3..6)), ids(&l, 3..6));
        // Again changes nothing: a caller re-stating its set is not a new announcement.
        assert!(a.add(ids(&l, 3..6)).is_empty());
        assert_eq!(a.add(ids(&l, 5..8)), ids(&l, 6..8));
        assert_eq!(a.snapshot(), Some(vec![3, 4, 5, 6, 7]));
        // We have 0..5 and 10: of what is advertised, 3 and 4 go out.
        let have = [0b1111_1000, 0b0010_0000];
        assert_eq!(ones(&a.announced(&have)), vec![3, 4]);
        assert_eq!(ones(&a.announced(&HAVE_ALL)), vec![3, 4, 5, 6, 7]);
    }

    #[test]
    fn holding_back_under_the_default_leaves_the_rest_advertised() {
        let l = lengths();
        let a = AdvertisedPieces::new(false, &l);
        assert_eq!(a.remove(ids(&l, 3..6)), 3);
        assert_eq!(a.remove(ids(&l, 3..6)), 0);
        assert_eq!(
            ones(&a.announced(&HAVE_ALL)),
            vec![0, 1, 2, 6, 7, 8, 9, 10, 11]
        );
        // The spare bits stay clear.
        assert_eq!(a.announced(&HAVE_ALL)[1] & 0x0f, 0);
        assert_eq!(a.add(ids(&l, 0..12)), ids(&l, 3..6));
        assert_eq!(ones(&a.announced(&HAVE_ALL)), (0..12).collect::<Vec<_>>());
    }
}
