//! Application-owned room roster and lifecycle primitives.
//!
//! This crate stores room membership, readiness and a small lifecycle state machine. It does not
//! allocate rooms to processes, synchronize them across nodes, send network messages, or prescribe
//! game-specific room data.

#![deny(missing_docs)]

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;

/// Configuration for one [`Room`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RoomConfig {
    /// Maximum number of simultaneous members.
    pub capacity: usize,
    /// Minimum number of members required by [`Room::start`].
    pub minimum_to_start: usize,
    /// Whether every member must be ready before the room starts.
    pub require_all_ready: bool,
    /// Whether new members may join after the room becomes active.
    pub allow_join_in_progress: bool,
}

impl Default for RoomConfig {
    fn default() -> Self {
        Self {
            capacity: 4,
            minimum_to_start: 1,
            require_all_ready: true,
            allow_join_in_progress: false,
        }
    }
}

impl RoomConfig {
    /// Validates capacity and start requirements.
    pub fn validate(&self) -> RoomResult<()> {
        if self.capacity == 0 {
            return Err(RoomError::InvalidConfig("capacity must be positive"));
        }
        if self.minimum_to_start == 0 || self.minimum_to_start > self.capacity {
            return Err(RoomError::InvalidConfig(
                "minimum_to_start must be in 1..=capacity",
            ));
        }
        Ok(())
    }
}

/// Current lifecycle phase of a room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RoomPhase {
    /// Members may join and update readiness.
    Open,
    /// The application has started the room's activity.
    Active,
    /// The room is terminal and accepts no further activity.
    Closed,
}

/// Stored state for one room member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomMember<D> {
    /// Application-owned member data.
    pub data: D,
    /// Readiness used by the default start policy.
    pub ready: bool,
    joined_order: u128,
}

/// Information returned after a member leaves a room.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomDeparture<M, D> {
    /// Application-owned data removed with the member.
    pub data: D,
    /// New leader selected by earliest remaining join order.
    pub new_leader: Option<M>,
    /// Whether the room has no remaining members.
    pub empty: bool,
}

/// Result returned by room operations.
pub type RoomResult<T> = std::result::Result<T, RoomError>;

/// Validation and lifecycle failures returned by [`Room`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RoomError {
    /// Room configuration is internally inconsistent.
    InvalidConfig(&'static str),
    /// The member is already present.
    AlreadyMember,
    /// The requested member is absent.
    MemberNotFound,
    /// The room has reached its configured capacity.
    Full,
    /// The operation requires an open room.
    NotOpen,
    /// Too few members are present to start.
    NotEnoughMembers {
        /// Required member count.
        minimum: usize,
        /// Current member count.
        actual: usize,
    },
    /// At least one member has not become ready.
    MembersNotReady,
}

impl fmt::Display for RoomError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid room config: {message}"),
            Self::AlreadyMember => formatter.write_str("member already joined the room"),
            Self::MemberNotFound => formatter.write_str("room member was not found"),
            Self::Full => formatter.write_str("room is full"),
            Self::NotOpen => formatter.write_str("room is not open"),
            Self::NotEnoughMembers { minimum, actual } => write!(
                formatter,
                "room requires at least {minimum} members but has {actual}"
            ),
            Self::MembersNotReady => formatter.write_str("not all room members are ready"),
        }
    }
}

impl std::error::Error for RoomError {}

/// In-memory room roster with deterministic leader succession.
///
/// The type is intentionally not internally synchronized. Keep it behind a scene mailbox or an
/// application-owned lock when multiple tasks can access it.
pub struct Room<I, M, D>
where
    M: Clone + Eq + Hash,
{
    id: I,
    config: RoomConfig,
    phase: RoomPhase,
    leader: Option<M>,
    members: HashMap<M, RoomMember<D>>,
    next_join_order: u128,
}

impl<I, M, D> Room<I, M, D>
where
    M: Clone + Eq + Hash,
{
    /// Creates an empty open room.
    pub fn new(id: I, config: RoomConfig) -> RoomResult<Self> {
        config.validate()?;
        Ok(Self {
            id,
            config,
            phase: RoomPhase::Open,
            leader: None,
            members: HashMap::with_capacity(config.capacity),
            next_join_order: 0,
        })
    }

    /// Returns the application-owned room identifier.
    pub fn id(&self) -> &I {
        &self.id
    }

    /// Returns the immutable room configuration.
    pub fn config(&self) -> &RoomConfig {
        &self.config
    }

    /// Returns the current lifecycle phase.
    pub fn phase(&self) -> RoomPhase {
        self.phase
    }

    /// Returns the current leader, if the room is non-empty.
    pub fn leader(&self) -> Option<&M> {
        self.leader.as_ref()
    }

    /// Returns the number of current members.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Returns true when no members remain.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Returns true when the member is present.
    pub fn contains(&self, member: &M) -> bool {
        self.members.contains_key(member)
    }

    /// Returns one member's stored state.
    pub fn member(&self, member: &M) -> Option<&RoomMember<D>> {
        self.members.get(member)
    }

    /// Returns mutable application-owned member state.
    pub fn member_mut(&mut self, member: &M) -> Option<&mut RoomMember<D>> {
        self.members.get_mut(member)
    }

    /// Iterates over member identifiers and stored state in unspecified order.
    pub fn members(&self) -> impl Iterator<Item = (&M, &RoomMember<D>)> {
        self.members.iter()
    }

    /// Returns member identifiers ordered by join time.
    pub fn members_in_join_order(&self) -> Vec<&M> {
        let mut members = self.members.iter().collect::<Vec<_>>();
        members.sort_unstable_by_key(|(_, member)| member.joined_order);
        members.into_iter().map(|(id, _)| id).collect()
    }

    /// Adds a member and makes the first member the leader.
    pub fn join(&mut self, member: M, data: D) -> RoomResult<()> {
        if self.phase == RoomPhase::Closed
            || (self.phase == RoomPhase::Active && !self.config.allow_join_in_progress)
        {
            return Err(RoomError::NotOpen);
        }
        if self.members.contains_key(&member) {
            return Err(RoomError::AlreadyMember);
        }
        if self.members.len() >= self.config.capacity {
            return Err(RoomError::Full);
        }

        let join_order = self.next_join_order;
        self.next_join_order = self.next_join_order.saturating_add(1);
        if self.leader.is_none() {
            self.leader = Some(member.clone());
        }
        self.members.insert(
            member,
            RoomMember {
                data,
                ready: false,
                joined_order: join_order,
            },
        );
        Ok(())
    }

    /// Removes a member and transfers leadership to the earliest remaining member.
    pub fn leave(&mut self, member: &M) -> RoomResult<RoomDeparture<M, D>> {
        let removed = self
            .members
            .remove(member)
            .ok_or(RoomError::MemberNotFound)?;
        if self.leader.as_ref() == Some(member) {
            self.leader = self
                .members
                .iter()
                .min_by_key(|(_, member)| member.joined_order)
                .map(|(id, _)| id.clone());
        }
        Ok(RoomDeparture {
            data: removed.data,
            new_leader: self.leader.clone(),
            empty: self.members.is_empty(),
        })
    }

    /// Explicitly transfers leadership to an existing member.
    pub fn transfer_leader(&mut self, member: &M) -> RoomResult<()> {
        if !self.members.contains_key(member) {
            return Err(RoomError::MemberNotFound);
        }
        self.leader = Some(member.clone());
        Ok(())
    }

    /// Updates one member's readiness while the room is open.
    pub fn set_ready(&mut self, member: &M, ready: bool) -> RoomResult<()> {
        if self.phase != RoomPhase::Open {
            return Err(RoomError::NotOpen);
        }
        self.members
            .get_mut(member)
            .ok_or(RoomError::MemberNotFound)?
            .ready = ready;
        Ok(())
    }

    /// Returns the number of ready members.
    pub fn ready_count(&self) -> usize {
        self.members.values().filter(|member| member.ready).count()
    }

    /// Returns true when the default start policy currently succeeds.
    pub fn can_start(&self) -> bool {
        self.phase == RoomPhase::Open
            && self.members.len() >= self.config.minimum_to_start
            && (!self.config.require_all_ready || self.ready_count() == self.members.len())
    }

    /// Transitions an eligible open room into the active phase.
    pub fn start(&mut self) -> RoomResult<()> {
        if self.phase != RoomPhase::Open {
            return Err(RoomError::NotOpen);
        }
        if self.members.len() < self.config.minimum_to_start {
            return Err(RoomError::NotEnoughMembers {
                minimum: self.config.minimum_to_start,
                actual: self.members.len(),
            });
        }
        if self.config.require_all_ready && self.ready_count() != self.members.len() {
            return Err(RoomError::MembersNotReady);
        }
        self.phase = RoomPhase::Active;
        Ok(())
    }

    /// Permanently closes the room.
    pub fn close(&mut self) {
        self.phase = RoomPhase::Closed;
    }
}
