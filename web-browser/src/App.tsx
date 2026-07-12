import "./App.css";
import { useWebSocket } from "./hooks/useWebSocket";

function App() {
  const { sendAndWait, isConnected, reconnect } = useWebSocket(
    "ws://localhost:8080/signal",
    {
      onOpen: async () => {
        const hub = await sendAndWait({
          type: "getLeastLoadedConsumerHub",
        });
        console.log(hub);
      },
      onClose: () => {
        console.log("closed");
        setTimeout(reconnect, 3000);
      },
      onMessage: (event) => {
        
      },
    },
  );

  return (
    <div className="App">{isConnected ? "connected" : "disconnected"}</div>
  );
}

export default App;
