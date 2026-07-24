use std::collections::HashSet;

use elura_aoi::{AoiConfig, AoiError, AoiGrid, AoiIndex, Point2};

fn grid() -> AoiGrid<u64> {
    let mut config = AoiConfig::default();
    config.cell_size = 10.0;
    config.max_query_cells = 100;
    AoiGrid::new(config).unwrap()
}

fn set(values: Vec<u64>) -> HashSet<u64> {
    values.into_iter().collect()
}

#[test]
fn queries_exact_circular_visibility_across_cells() {
    let mut grid = grid();
    grid.insert(1, Point2::new(0.0, 0.0)).unwrap();
    grid.insert(2, Point2::new(9.0, 0.0)).unwrap();
    grid.insert(3, Point2::new(10.0, 0.0)).unwrap();
    grid.insert(4, Point2::new(8.0, 8.0)).unwrap();

    assert_eq!(set(grid.visible_to(&1, 10.0).unwrap()), set(vec![2, 3]));
}

#[test]
fn reports_entered_and_left_entities_after_movement() {
    let mut grid = grid();
    grid.insert(1, Point2::new(0.0, 0.0)).unwrap();
    grid.insert(2, Point2::new(2.0, 0.0)).unwrap();
    grid.insert(3, Point2::new(18.0, 0.0)).unwrap();

    let delta = grid.relocate(&1, Point2::new(16.0, 0.0), 5.0).unwrap();
    assert_eq!(set(delta.entered), set(vec![3]));
    assert_eq!(set(delta.left), set(vec![2]));
}

#[test]
fn updates_sparse_cells_and_removes_entities() {
    let mut grid = grid();
    assert_eq!(grid.upsert(1, Point2::new(-1.0, -1.0)).unwrap(), None);
    assert_eq!(
        grid.upsert(1, Point2::new(21.0, 0.0)).unwrap(),
        Some(Point2::new(-1.0, -1.0))
    );
    assert_eq!(grid.position(&1), Some(Point2::new(21.0, 0.0)));
    assert_eq!(grid.remove(&1).unwrap(), Point2::new(21.0, 0.0));
    assert!(grid.is_empty());
}

#[test]
fn rejects_invalid_positions_radii_and_duplicate_entities() {
    let mut grid = grid();
    grid.insert(1, Point2::new(0.0, 0.0)).unwrap();
    assert!(matches!(
        grid.insert(1, Point2::new(1.0, 1.0)),
        Err(AoiError::AlreadyExists)
    ));
    assert!(matches!(
        grid.insert(2, Point2::new(f64::NAN, 0.0)),
        Err(AoiError::InvalidPosition)
    ));
    assert!(matches!(
        grid.query(Point2::new(0.0, 0.0), -1.0),
        Err(AoiError::InvalidRadius)
    ));
}

#[test]
fn bounds_query_work() {
    let mut config = AoiConfig::default();
    config.cell_size = 1.0;
    config.max_query_cells = 4;
    let grid = AoiGrid::<u64>::new(config).unwrap();
    assert!(matches!(
        grid.query(Point2::new(0.0, 0.0), 2.0),
        Err(AoiError::QueryTooLarge { .. })
    ));
}

#[test]
fn grid_implements_the_extensible_index_contract() {
    fn visible<I>(index: &I, observer: &u64) -> Result<Vec<u64>, I::Error>
    where
        I: AoiIndex<u64>,
    {
        index.visible_to(observer, 10.0)
    }

    let mut grid = grid();
    AoiIndex::insert(&mut grid, 1, Point2::new(0.0, 0.0)).unwrap();
    AoiIndex::insert(&mut grid, 2, Point2::new(5.0, 0.0)).unwrap();

    assert_eq!(visible(&grid, &1).unwrap(), vec![2]);
    assert_eq!(AoiIndex::len(&grid), 2);
    assert!(!AoiIndex::is_empty(&grid));
}
