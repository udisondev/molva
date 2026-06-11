import { createRoot } from "react-dom/client";
import { App } from "./App";
import { startClient } from "./state/store";
import "./styles.css";

startClient();
createRoot(document.getElementById("root")!).render(<App />);
