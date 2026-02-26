import saveLabelVersion from "./studio-save-label-version.js";
import saveConfiguration from "./studio-save-configuration.js";
import listStudio from "./studio-list.js";
import renameConfiguration from "./studio-rename-configuration.js";
import renameLabel from "./studio-rename-label.js";
import hasContent from "./studio-has-content.js";
import getConfiguration from "./studio-configuration.js";

const notFound = () =>
  new Response(
    JSON.stringify({
      ok: false,
      error: "not_found",
      message: "Unknown studio endpoint.",
    }),
    { status: 404, headers: { "Content-Type": "application/json" } }
  );

const resolvePath = (arg) => {
  try {
    if (typeof arg?.url === "string") return new URL(arg.url).pathname;
  } catch {}
  return arg?.path || "";
};

export default async (arg) => {
  const path = resolvePath(arg).toLowerCase();

  if (path.endsWith("/studio/save-label-version")) return saveLabelVersion(arg);
  if (path.endsWith("/studio/save-configuration")) return saveConfiguration(arg);
  if (path.endsWith("/studio/list")) return listStudio(arg);
  if (path.endsWith("/studio/rename-configuration")) return renameConfiguration(arg);
  if (path.endsWith("/studio/rename-label")) return renameLabel(arg);
  if (path.endsWith("/studio/has-content")) return hasContent(arg);
  if (path.endsWith("/studio/configuration")) return getConfiguration(arg);

  return notFound();
};
