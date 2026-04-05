//! Auto-generated GraphQL schema from graph schema.
//!
//! Converts CoordiNode's graph schema (labels, edge types, property definitions)
//! into a GraphQL SDL string. The generated schema follows Relay connection spec
//! for pagination and includes auto-generated query, filter, and ordering types.
//!
//! Anti-pattern: "Hand-writing GraphQL resolvers" — resolvers are generated, not coded.

pub mod sdl;

pub use sdl::generate_graphql_sdl;
