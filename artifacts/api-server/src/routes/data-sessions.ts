import { stat } from "node:fs/promises";
import { Router, type IRouter } from "express";
import {
  ApplyCleanupParams,
  ApplyCleanupResponse,
  CloseDataSessionParams,
  CreateDataSessionResponse,
  ExportCleanedDataParams,
  ExportCleanedDataResponse,
  GetDataSessionQueryParams,
  GetDataSessionResponse,
  GetFftAuditParams,
  GetFftAuditResponse,
  GetSessionAnalyticsParams,
  GetSessionAnalyticsResponse,
  GetSessionFlightsQueryParams,
  GetSessionFlightsResponse,
  PreviewCleanupBody,
  PreviewCleanupParams,
  PreviewCleanupResponse,
} from "@workspace/api-zod";
import {
  applyCleanup,
  closeDataSession,
  createDataSession,
  getDataSession,
  getDownload,
  getFftAudit,
  getSessionAnalytics,
  getSessionFlights,
  previewCleanup,
} from "../lib/datasetSessions";

const router: IRouter = Router();

router.post("/data-sessions", async (req, res): Promise<void> => {
  try {
    const result = await createDataSession(
      req,
      req.get("x-filename") ?? "aircraft-data.json",
      Number(req.get("content-length")) || undefined,
    );
    res.status(201).json(CreateDataSessionResponse.parse(result));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to import this JSON file.";
    req.log.warn({ message }, "Aircraft data import failed");
    res.status(400).json({ error: message });
  }
});

router.get("/data-sessions", (req, res): void => {
  const parsed = GetDataSessionQueryParams.safeParse(req.query);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }
  const session = getDataSession(parsed.data.sessionId);
  if (!session) {
    res.status(404).json({ error: "This temporary data session has expired or was closed." });
    return;
  }
  res.json(GetDataSessionResponse.parse(session));
});

router.get("/data-sessions/flights", (req, res): void => {
  const parsed = GetSessionFlightsQueryParams.safeParse(req.query);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }
  const { sessionId, search, tailNumber, condition, page = 1, limit = 25 } = parsed.data;
  const result = getSessionFlights(sessionId, { search, tailNumber, condition, page, limit });
  if (!result) {
    res.status(404).json({ error: "This temporary data session has expired or was closed." });
    return;
  }
  res.json(GetSessionFlightsResponse.parse(result));
});

router.get("/data-sessions/:sessionId/analytics", (req, res): void => {
  const parsed = GetSessionAnalyticsParams.safeParse(req.params);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }
  const result = getSessionAnalytics(parsed.data.sessionId);
  if (!result) {
    res.status(404).json({ error: "This temporary data session has expired or was closed." });
    return;
  }
  res.json(GetSessionAnalyticsResponse.parse(result));
});

router.get("/data-sessions/:sessionId/fft-audit", (req, res): void => {
  const parsed = GetFftAuditParams.safeParse(req.params);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }
  const result = getFftAudit(parsed.data.sessionId);
  if (!result) {
    res.status(404).json({ error: "This temporary data session has expired or was closed." });
    return;
  }
  res.json(GetFftAuditResponse.parse(result));
});

router.post("/data-sessions/:sessionId/clean/preview", async (req, res): Promise<void> => {
  const parsedParams = PreviewCleanupParams.safeParse(req.params);
  const parsedBody = PreviewCleanupBody.safeParse(req.body);
  if (!parsedParams.success) {
    res.status(400).json({
      error: parsedParams.error.message,
    });
    return;
  }
  if (!parsedBody.success) {
    res.status(400).json({
      error: parsedBody.error.message,
    });
    return;
  }
  try {
    const result = await previewCleanup(parsedParams.data.sessionId, parsedBody.data);
    if (!result) {
      res.status(404).json({ error: "This temporary data session has expired or was closed." });
      return;
    }
    res.json(PreviewCleanupResponse.parse(result));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Cleanup preview failed.";
    req.log.error({ message }, "Aircraft cleanup preview failed");
    res.status(500).json({ error: message });
  }
});

router.post("/data-sessions/:sessionId/clean/apply", async (req, res): Promise<void> => {
  const parsed = ApplyCleanupParams.safeParse(req.params);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }
  try {
    const result = await applyCleanup(parsed.data.sessionId);
    if (!result) {
      res.status(404).json({ error: "This temporary data session has expired or was closed." });
      return;
    }
    res.json(ApplyCleanupResponse.parse(result));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Cleanup could not be applied.";
    res.status(409).json({ error: message });
  }
});

router.get("/data-sessions/:sessionId/export", async (req, res): Promise<void> => {
  const parsed = ExportCleanedDataParams.safeParse(req.params);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }
  const download = getDownload(parsed.data.sessionId);
  if (!download) {
    res.status(409).json({ error: "Apply a cleanup preview before exporting." });
    return;
  }
  const details = await stat(download.path);
  res.json(
    ExportCleanedDataResponse.parse({
      downloadUrl: `/api/data-sessions/${parsed.data.sessionId}/download`,
      filename: download.filename,
      bytes: details.size,
    }),
  );
});

router.get("/data-sessions/:sessionId/download", async (req, res): Promise<void> => {
  const download = getDownload(String(req.params.sessionId ?? ""));
  if (!download) {
    res.status(404).json({ error: "The cleaned export is not available in this session." });
    return;
  }
  res.download(download.path, download.filename, (error) => {
    if (error) req.log.warn({ error }, "Cleaned dataset download ended with an error");
  });
});

router.delete("/data-sessions/:sessionId", async (req, res): Promise<void> => {
  const parsed = CloseDataSessionParams.safeParse(req.params);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }
  await closeDataSession(parsed.data.sessionId);
  res.sendStatus(204);
});

export default router;
