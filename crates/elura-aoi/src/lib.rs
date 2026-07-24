//! Generic two-dimensional area-of-interest indexing.
//!
//! [`AoiGrid`] tracks application-owned entity identifiers in a sparse uniform grid and computes
//! circular visibility queries and movement deltas. It deliberately does not send events, impose a
//! coordinate system, or synchronize access across tasks.
//!
//! Applications that need a different spatial algorithm can implement [`AoiIndex`] and keep the
//! same upper-layer composition.

#![deny(missing_docs)]

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;

/// Two-dimensional world position.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point2 {
    /// Horizontal coordinate chosen by the application.
    pub x: f64,
    /// Second horizontal coordinate chosen by the application.
    pub y: f64,
}

impl Point2 {
    /// Creates a point without assigning coordinate-system semantics.
    pub const fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    fn validate(self) -> AoiResult<()> {
        if self.x.is_finite() && self.y.is_finite() {
            Ok(())
        } else {
            Err(AoiError::InvalidPosition)
        }
    }

    fn distance_squared(self, other: Self) -> f64 {
        let x = self.x - other.x;
        let y = self.y - other.y;
        x.mul_add(x, y * y)
    }
}

/// Sparse-grid limits shared by all entities in one [`AoiGrid`].
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub struct AoiConfig {
    /// Width and height of one square index cell.
    pub cell_size: f64,
    /// Maximum number of cells one query may scan.
    pub max_query_cells: usize,
}

impl Default for AoiConfig {
    fn default() -> Self {
        Self {
            cell_size: 32.0,
            max_query_cells: 4_096,
        }
    }
}

impl AoiConfig {
    /// Validates finite grid dimensions and query limits.
    pub fn validate(&self) -> AoiResult<()> {
        if !self.cell_size.is_finite() || self.cell_size <= 0.0 {
            return Err(AoiError::InvalidConfig(
                "cell_size must be finite and positive",
            ));
        }
        if self.max_query_cells == 0 {
            return Err(AoiError::InvalidConfig("max_query_cells must be positive"));
        }
        Ok(())
    }
}

/// Entity identifiers that entered or left visibility after movement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibilityDelta<K> {
    /// Entities newly visible from the moved entity's position.
    pub entered: Vec<K>,
    /// Entities no longer visible from the moved entity's position.
    pub left: Vec<K>,
}

/// Result returned by AOI operations.
pub type AoiResult<T> = std::result::Result<T, AoiError>;

/// Validation, lookup and bounded-query failures.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AoiError {
    /// Grid configuration is invalid.
    InvalidConfig(&'static str),
    /// A position contains a non-finite or unsupported coordinate.
    InvalidPosition,
    /// A query radius is negative or non-finite.
    InvalidRadius,
    /// The entity is already indexed.
    AlreadyExists,
    /// The entity is not indexed.
    NotFound,
    /// A query would scan more cells than configured.
    QueryTooLarge {
        /// Number of cells required by the query bounding box.
        cells: usize,
        /// Configured maximum cell count.
        maximum: usize,
    },
}

impl fmt::Display for AoiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid AOI config: {message}"),
            Self::InvalidPosition => formatter.write_str("invalid AOI position"),
            Self::InvalidRadius => formatter.write_str("invalid AOI query radius"),
            Self::AlreadyExists => formatter.write_str("AOI entity already exists"),
            Self::NotFound => formatter.write_str("AOI entity was not found"),
            Self::QueryTooLarge { cells, maximum } => {
                write!(
                    formatter,
                    "AOI query needs {cells} cells, maximum is {maximum}"
                )
            }
        }
    }
}

impl std::error::Error for AoiError {}

/// Application-extensible area-of-interest index.
///
/// The associated position and error types let applications provide indexes with different
/// coordinate representations and algorithm-specific failures. [`AoiGrid`] is the built-in sparse
/// uniform-grid implementation.
pub trait AoiIndex<K> {
    /// Position representation understood by this index.
    type Position;

    /// Error returned by index operations.
    type Error;

    /// Returns the number of indexed entities.
    fn len(&self) -> usize;

    /// Returns true when no entities are indexed.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the current entity position.
    fn position(&self, entity: &K) -> Option<Self::Position>;

    /// Inserts a previously unknown entity.
    fn insert(&mut self, entity: K, position: Self::Position) -> Result<(), Self::Error>;

    /// Inserts or moves an entity, returning its previous position when present.
    fn upsert(
        &mut self,
        entity: K,
        position: Self::Position,
    ) -> Result<Option<Self::Position>, Self::Error>;

    /// Moves an indexed entity and returns its previous position.
    fn move_entity(
        &mut self,
        entity: &K,
        position: Self::Position,
    ) -> Result<Self::Position, Self::Error>;

    /// Moves an entity and reports which other entities entered or left its view.
    fn relocate(
        &mut self,
        entity: &K,
        position: Self::Position,
        radius: f64,
    ) -> Result<VisibilityDelta<K>, Self::Error>;

    /// Removes and returns an indexed entity's final position.
    fn remove(&mut self, entity: &K) -> Result<Self::Position, Self::Error>;

    /// Returns entities inside an area around an arbitrary position.
    fn query(&self, center: Self::Position, radius: f64) -> Result<Vec<K>, Self::Error>;

    /// Returns other entities visible to one indexed entity.
    fn visible_to(&self, entity: &K, radius: f64) -> Result<Vec<K>, Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Cell {
    x: i32,
    y: i32,
}

/// Sparse uniform-grid index for application-owned entity identifiers.
///
/// Query result order is unspecified. The type is intentionally not internally synchronized; one
/// scene task can own it directly, or the application can place it behind its own lock.
pub struct AoiGrid<K>
where
    K: Clone + Eq + Hash,
{
    config: AoiConfig,
    positions: HashMap<K, Point2>,
    cells: HashMap<Cell, HashSet<K>>,
}

impl<K> AoiGrid<K>
where
    K: Clone + Eq + Hash,
{
    /// Creates an empty AOI index.
    pub fn new(config: AoiConfig) -> AoiResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            positions: HashMap::new(),
            cells: HashMap::new(),
        })
    }

    /// Returns the immutable grid configuration.
    pub fn config(&self) -> &AoiConfig {
        &self.config
    }

    /// Returns the number of indexed entities.
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Returns true when no entities are indexed.
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// Returns the current entity position.
    pub fn position(&self, entity: &K) -> Option<Point2> {
        self.positions.get(entity).copied()
    }

    /// Iterates over entity identifiers and positions in unspecified order.
    pub fn entities(&self) -> impl Iterator<Item = (&K, Point2)> {
        self.positions
            .iter()
            .map(|(entity, position)| (entity, *position))
    }

    /// Inserts a previously unknown entity.
    pub fn insert(&mut self, entity: K, position: Point2) -> AoiResult<()> {
        if self.positions.contains_key(&entity) {
            return Err(AoiError::AlreadyExists);
        }
        let cell = self.cell(position)?;
        self.cells.entry(cell).or_default().insert(entity.clone());
        self.positions.insert(entity, position);
        Ok(())
    }

    /// Inserts or moves an entity, returning its previous position when present.
    pub fn upsert(&mut self, entity: K, position: Point2) -> AoiResult<Option<Point2>> {
        match self.positions.get(&entity).copied() {
            Some(previous) => {
                self.move_entity(&entity, position)?;
                Ok(Some(previous))
            }
            None => {
                self.insert(entity, position)?;
                Ok(None)
            }
        }
    }

    /// Moves an indexed entity and returns its previous position.
    pub fn move_entity(&mut self, entity: &K, position: Point2) -> AoiResult<Point2> {
        let previous = self
            .positions
            .get(entity)
            .copied()
            .ok_or(AoiError::NotFound)?;
        let new_cell = self.cell(position)?;
        let previous_cell = self.cell(previous)?;
        if previous_cell != new_cell {
            self.remove_from_cell(previous_cell, entity);
            self.cells
                .entry(new_cell)
                .or_default()
                .insert(entity.clone());
        }
        self.positions.insert(entity.clone(), position);
        Ok(previous)
    }

    /// Moves an entity and computes which other entities entered or left its circular view.
    pub fn relocate(
        &mut self,
        entity: &K,
        position: Point2,
        radius: f64,
    ) -> AoiResult<VisibilityDelta<K>> {
        let previous = self
            .positions
            .get(entity)
            .copied()
            .ok_or(AoiError::NotFound)?;
        position.validate()?;
        let before = self
            .query(previous, radius)?
            .into_iter()
            .filter(|candidate| candidate != entity)
            .collect::<HashSet<_>>();
        let after = self
            .query(position, radius)?
            .into_iter()
            .filter(|candidate| candidate != entity)
            .collect::<HashSet<_>>();
        self.move_entity(entity, position)?;
        Ok(VisibilityDelta {
            entered: after.difference(&before).cloned().collect(),
            left: before.difference(&after).cloned().collect(),
        })
    }

    /// Removes and returns an indexed entity's final position.
    pub fn remove(&mut self, entity: &K) -> AoiResult<Point2> {
        let position = self.positions.remove(entity).ok_or(AoiError::NotFound)?;
        let cell = self.cell(position)?;
        self.remove_from_cell(cell, entity);
        Ok(position)
    }

    /// Returns entities inside a circular area around an arbitrary position.
    pub fn query(&self, center: Point2, radius: f64) -> AoiResult<Vec<K>> {
        center.validate()?;
        if !radius.is_finite() || radius < 0.0 {
            return Err(AoiError::InvalidRadius);
        }
        let minimum = Point2::new(center.x - radius, center.y - radius);
        let maximum = Point2::new(center.x + radius, center.y + radius);
        let minimum_cell = self.cell(minimum)?;
        let maximum_cell = self.cell(maximum)?;
        let width = i64::from(maximum_cell.x) - i64::from(minimum_cell.x) + 1;
        let height = i64::from(maximum_cell.y) - i64::from(minimum_cell.y) + 1;
        let cells = usize::try_from(width)
            .ok()
            .and_then(|width| {
                usize::try_from(height)
                    .ok()
                    .and_then(|height| width.checked_mul(height))
            })
            .ok_or(AoiError::QueryTooLarge {
                cells: usize::MAX,
                maximum: self.config.max_query_cells,
            })?;
        if cells > self.config.max_query_cells {
            return Err(AoiError::QueryTooLarge {
                cells,
                maximum: self.config.max_query_cells,
            });
        }

        let radius_squared = radius * radius;
        let mut visible = Vec::new();
        for x in minimum_cell.x..=maximum_cell.x {
            for y in minimum_cell.y..=maximum_cell.y {
                let Some(entities) = self.cells.get(&Cell { x, y }) else {
                    continue;
                };
                visible.extend(entities.iter().filter_map(|entity| {
                    let position = self.positions.get(entity)?;
                    (center.distance_squared(*position) <= radius_squared).then(|| entity.clone())
                }));
            }
        }
        Ok(visible)
    }

    /// Returns other entities visible to one indexed entity.
    pub fn visible_to(&self, entity: &K, radius: f64) -> AoiResult<Vec<K>> {
        let position = self
            .positions
            .get(entity)
            .copied()
            .ok_or(AoiError::NotFound)?;
        Ok(self
            .query(position, radius)?
            .into_iter()
            .filter(|candidate| candidate != entity)
            .collect())
    }

    fn cell(&self, position: Point2) -> AoiResult<Cell> {
        position.validate()?;
        let x = (position.x / self.config.cell_size).floor();
        let y = (position.y / self.config.cell_size).floor();
        if x < f64::from(i32::MIN)
            || x > f64::from(i32::MAX)
            || y < f64::from(i32::MIN)
            || y > f64::from(i32::MAX)
        {
            return Err(AoiError::InvalidPosition);
        }
        Ok(Cell {
            x: x as i32,
            y: y as i32,
        })
    }

    fn remove_from_cell(&mut self, cell: Cell, entity: &K) {
        let empty = self.cells.get_mut(&cell).is_some_and(|entities| {
            entities.remove(entity);
            entities.is_empty()
        });
        if empty {
            self.cells.remove(&cell);
        }
    }
}

impl<K> AoiIndex<K> for AoiGrid<K>
where
    K: Clone + Eq + Hash,
{
    type Position = Point2;
    type Error = AoiError;

    fn len(&self) -> usize {
        AoiGrid::len(self)
    }

    fn position(&self, entity: &K) -> Option<Self::Position> {
        AoiGrid::position(self, entity)
    }

    fn insert(&mut self, entity: K, position: Self::Position) -> Result<(), Self::Error> {
        AoiGrid::insert(self, entity, position)
    }

    fn upsert(
        &mut self,
        entity: K,
        position: Self::Position,
    ) -> Result<Option<Self::Position>, Self::Error> {
        AoiGrid::upsert(self, entity, position)
    }

    fn move_entity(
        &mut self,
        entity: &K,
        position: Self::Position,
    ) -> Result<Self::Position, Self::Error> {
        AoiGrid::move_entity(self, entity, position)
    }

    fn relocate(
        &mut self,
        entity: &K,
        position: Self::Position,
        radius: f64,
    ) -> Result<VisibilityDelta<K>, Self::Error> {
        AoiGrid::relocate(self, entity, position, radius)
    }

    fn remove(&mut self, entity: &K) -> Result<Self::Position, Self::Error> {
        AoiGrid::remove(self, entity)
    }

    fn query(&self, center: Self::Position, radius: f64) -> Result<Vec<K>, Self::Error> {
        AoiGrid::query(self, center, radius)
    }

    fn visible_to(&self, entity: &K, radius: f64) -> Result<Vec<K>, Self::Error> {
        AoiGrid::visible_to(self, entity, radius)
    }
}
