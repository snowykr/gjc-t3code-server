const SYNTHETIC_PROVIDER_ID = "gajae-code";

export interface GjcSelectableModel {
  readonly provider: string;
  readonly id: string;
  readonly name?: string;
}

/** Returns a configured profile name without imposing syntax on user-owned keys. */
export function profileNameForSelection(input: {
  readonly model?: string | undefined;
  readonly modelProfile?: string | undefined;
}): string | undefined {
  const prefix = `${SYNTHETIC_PROVIDER_ID}/`;
  const profile = input.model?.startsWith(prefix) ? input.model.slice(prefix.length) : undefined;
  if (profile !== undefined) return profile;
  const configured = input.modelProfile?.trim();
  return configured || undefined;
}

/** Resolves only a concrete model exposed by the current session. */
export function findAvailableConcreteModel<T extends GjcSelectableModel>(
  models: readonly T[],
  modelId: string,
): T | undefined {
  if (profileNameForSelection({ model: modelId }) !== undefined) return undefined;
  const separator = modelId.indexOf("/");
  if (separator <= 0 || separator === modelId.length - 1) return undefined;
  const provider = modelId.slice(0, separator);
  const id = modelId.slice(separator + 1);
  return models.find((model) => model.provider === provider && model.id === id);
}

export function hasSyntheticProfileNamespaceCollision(
  models: readonly Pick<GjcSelectableModel, "provider">[],
  configuredProviderIds: readonly string[],
): boolean {
  return (
    models.some((model) => model.provider === SYNTHETIC_PROVIDER_ID) ||
    configuredProviderIds.includes(SYNTHETIC_PROVIDER_ID)
  );
}
