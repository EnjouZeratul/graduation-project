let socket: WebSocket | null = null;
let reconnectTimer: number | null = null;

type Callback = (data: any) => void;

let listeners: Callback[] = [];

function openSocket() {
  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
    return;
  }
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const wsUrl = `${protocol}://${window.location.host}/ws/warnings`;
  socket = new WebSocket(wsUrl);

  socket.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      listeners.forEach((cb) => cb(data));
    } catch {
      // ignore
    }
  };

  socket.onclose = () => {
    socket = null;
    if (reconnectTimer !== null) {
      window.clearTimeout(reconnectTimer);
    }
    reconnectTimer = window.setTimeout(() => {
      openSocket();
    }, 5000);
  };
}

export function connectWarningsWS(onMessage: Callback) {
  if (!listeners.includes(onMessage)) {
    listeners.push(onMessage);
  }
  openSocket();
}

export function removeListener(cb: Callback) {
  listeners = listeners.filter((fn) => fn !== cb);
}

