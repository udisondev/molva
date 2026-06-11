// Мост preload: реквизиты подключения к ядру и буфер обмена.
interface Window {
  molva: {
    conn(): Promise<{ addr: string; token: string }>;
    copy(text: string): Promise<void>;
  };
}
