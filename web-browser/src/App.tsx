import "./App.css";
import VideoPlayer from "./components/VideoPlayer";
import { useMediasoupConnection } from "./hooks/useMediasoupConnection";

const cctvList = Array.from({ length: 12 }, (_, i) => `video${i}`);

function App() {
  const { streams } = useMediasoupConnection("ws://localhost:3000", cctvList);

  return (
    <>
      {cctvList.map((cctvId: string) => (
        <VideoPlayer key={cctvId} stream={streams[cctvId]} />
      ))}
    </>
  );
}

export default App;
