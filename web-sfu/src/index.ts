import router from "./router";
import gateway from "./gateway";

(async () => {
  await router.init();
  for (let i = 0; i < 20; i++) {
    await router.listenProducer("127.0.0.1", 25000 + i);
  }
  await gateway.init();
})().catch((err) => {
  console.error(err);
});
