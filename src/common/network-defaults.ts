// Shared by the server and the clients, which otherwise don't depend on each
// other: a client program shouldn't need the server's code.

/** The TCP port used unless told otherwise. */
export const DEFAULT_PORT = 7400;

/**
 * The address used unless told otherwise: 127.0.0.1, this computer only.
 * EventRail has no authentication or encryption, so listening on a network
 * must be a deliberate choice.
 */
export const DEFAULT_HOST = "127.0.0.1";
