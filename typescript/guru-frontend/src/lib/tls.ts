/**
 * One hostname, optionally with a leading `*.` wildcard label. Deliberately
 * narrower than the RFC: the ACME order is built from this verbatim. Shared by
 * the remote functions' schema and the pod panel, which checks before saving.
 */
export const SNI_PATTERN =
	/^(\*\.)?[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$/i;
