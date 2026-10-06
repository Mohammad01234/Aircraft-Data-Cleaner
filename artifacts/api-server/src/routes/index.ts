import { Router, type IRouter } from "express";
import healthRouter from "./health";
import dataSessionsRouter from "./data-sessions";

const router: IRouter = Router();

router.use(healthRouter);
router.use(dataSessionsRouter);

export default router;
