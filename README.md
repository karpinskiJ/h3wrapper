# Scala Library with H3 Spatial Index and Apache Sedona

## Overview

This Scala library provides a set of utilities for working with spatial data using the H3 spatial index and Apache Sedona (formerly known as GeoSpark). H3 is a versatile hexagon-based spatial indexing system, and Apache Sedona is a distributed computing library for processing large-scale spatial data.

## Features

- **H3 Spatial Indexing:** Leverage the power of H3 to efficiently index and query spatial data using hexagons.
- **Integration with Apache Sedona:** Seamlessly integrate with Apache Sedona for distributed spatial data processing.
- **Spatial Operations:** Perform various spatial operations such as point-in-polygon, distance calculations, and more.
- **Scala API:** Utilize a user-friendly Scala API to easily incorporate spatial functionalities into your Scala applications.
## Predicate functions
- **pointInPolygonJoin**: Joining datasets based on the point belonging to one set to the polygon in another set.
- **geometryInsidePolygonJoin**: Joining datasets based on the polygon belonging to one set to the polygon in another set.
- **geometriesIntersectJoin**: Joining datasets based on the intersection of a polygon from one set with a polygon from another set.
- **getPointsInRangeFromPoints**: Joining datasets based on the distance between points occurring in the datasets.
- **getPointsInRangeFromPolygon**: Joining datasets based on the distance between a point in one dataset and a polygon in another.
- **getPolygonsInRangeFromPolygons**: Joining datasets based on the distance between a polygon in one dataset and a polygon in another.
## Getting Started

### Prerequisites

- Scala 2.12+
- Apache Sedona
- H3 Library

### Installation

To use this library in your Scala project, add the following dependency to your `build.sbt` file:

```scala
libraryDependencies += to be addded" %% "h3xwrapper" % "1.0.0"
