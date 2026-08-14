/**
 * GjcAdapter — shape type for the Gjc provider adapter.
 *
 * The driver model ({@link ../Drivers/GjcDriver}) bundles one adapter per
 * instance as a captured closure, so this module only retains the shape
 * interface as a naming anchor for the driver bundle.
 *
 * @module GjcAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * GjcAdapterShape — per-instance Gjc adapter contract.
 */
export interface GjcAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
