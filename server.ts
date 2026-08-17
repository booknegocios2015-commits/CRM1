import express, { Request, Response } from 'express';
import path from 'path';
import { createServer as createViteServer } from 'vite';
import dotenv from 'dotenv';
import { db } from './src/server/db';
import { deliverMessage } from './src/server/delivery';
import { verifyZernioSignature } from './src/lib/zernio/webhooks';
import { Channel, Provider } from './src/types';

dotenv.config();

async function startServer() {
  const app = express();
  const PORT = 3000;

  // Custom Raw Body Middleware for HMAC signature verification
  app.use(
    express.json({
      verify: (req: any, _res, buf) => {
        req.rawBody = buf;
      },
    })
  );

  // Health check
  app.get('/api/health', (_req, res) => {
    res.json({
      status: 'ok',
      service: 'multichannel-crm-backend',
      timestamp: new Date().toISOString(),
    });
  });

  // =========================================================================
  // FASE 3: INCOMING WEBHOOK PIPELINE (/api/webhooks/zernio)
  // Exact sequence specified in PDF Guide:
  // 1. Raw body HMAC check
  // 2. Claim event by event_id (idempotency)
  // 3. Match channel_accounts
  // 4. Resolve contact & identity, upsert conversation, insert message
  // 5. Enqueue AI background processing
  // 6. Return 200 immediately
  // =========================================================================
  app.post('/api/webhooks/zernio', async (req: any, res: Response) => {
    const rawBody = req.rawBody || JSON.stringify(req.body);
    const signature = req.headers['x-zernio-signature'] as string | undefined;

    // 1. Verify HMAC Signature
    const sigCheck = verifyZernioSignature(rawBody, signature);
    if (!sigCheck.valid) {
      console.warn('[Webhook Auth Failed]:', sigCheck.reason);
      return res.status(401).json({ error: 'Invalid HMAC signature', reason: sigCheck.reason });
    }

    const payload = req.body;
    const eventId = payload.event_id || payload.id || `evt_${Date.now()}_${Math.random().toString(36).substring(2, 6)}`;
    const eventType = payload.event_type || 'message.incoming';
    const channel: Channel = payload.channel || 'whatsapp';
    const provider: Provider = payload.provider || 'zernio';

    // 2. Claim Event for Idempotency
    const isFirstClaim = db.claimWebhookEvent({
      event_id: eventId,
      provider,
      channel,
      event_type: eventType,
      payload,
    });

    if (!isFirstClaim) {
      console.log(`[Webhook Idempotency]: Event ${eventId} already processed (retry ignored).`);
      return res.status(200).json({ status: 'duplicate_ignored', event_id: eventId });
    }

    // 3. Route by Account ID against channel_accounts
    const accountId = payload.account_id || `acc_${channel}_main`;
    const account = db.getChannelAccount(accountId) || db.getAccountByExternalId(payload.account_id);
    if (!account && process.env.NODE_ENV === 'production') {
      console.log(`[Webhook Ignored]: No channel_account matched for ${accountId}`);
      return res.status(200).json({ status: 'account_not_found_ignored' });
    }

    // Handle account status events
    if (eventType === 'account.connected' || eventType === 'account.disconnected') {
      if (account) {
        db.updateAccountStatus(account.id, eventType === 'account.connected' ? 'connected' : 'disconnected');
      }
      return res.status(200).json({ status: 'account_event_recorded' });
    }

    // 4. Resolve Contact & Contact Identity
    const sender = payload.sender || {};
    const externalUserId = sender.id || payload.from || `user_${Date.now()}`;
    const contact = db.resolveContact({
      channel,
      externalId: externalUserId,
      name: sender.name || payload.sender_name,
      handle: sender.handle || payload.sender_handle || externalUserId,
      phone: sender.phone || payload.sender_phone,
      avatarUrl: sender.avatar_url || payload.sender_avatar,
    });

    // Find or create conversation thread
    const threadExternalId = payload.thread_id || payload.conversation_id || `thread_${channel}_${externalUserId}`;
    let conversation = db.findConversationByProviderExternalId(provider, threadExternalId);

    const nowIso = new Date().toISOString();
    const messageData = payload.message || {};
    const messageBody = messageData.text?.body || messageData.body || payload.text || payload.body || 'Nuevo mensaje';
    const messageExtId = messageData.id || payload.message_id || `msg_in_${Date.now()}`;

    if (!conversation) {
      const newConvId = `conv_${channel}_${Date.now().toString(36)}`;
      conversation = db.upsertConversation({
        id: newConvId,
        channel,
        provider,
        external_id: threadExternalId,
        account_id: account?.id || accountId,
        participant_id: externalUserId,
        participant_name: contact.name,
        participant_handle: sender.handle || sender.phone || externalUserId,
        participant_picture: contact.avatar_url,
        last_message_at: nowIso,
        last_inbound_at: nowIso,
        unread_count: 1,
        ai_enabled: true,
        contact_id: contact.id,
        last_message_preview: messageBody,
        metadata: {
          ad_attribution: messageData.ad_attribution || payload.ad_attribution,
          lead_status: 'new',
          tags: [channel.toUpperCase()],
        },
      });
    }

    // Insert message into DB enforcing partial index
    db.insertMessage({
      conversation_id: conversation.id,
      channel,
      provider,
      external_id: messageExtId,
      direction: 'inbound',
      type: messageData.type || 'text',
      body: messageBody,
      status: 'delivered',
      raw_payload: payload,
      sent_at: nowIso,
      sender_name: contact.name,
      media_url: messageData.media?.url,
    });

    // 5. Trigger Agent Asynchronously in background (Dynamic import inside background callback per PDF rule)
    const activeConvId = conversation.id;
    setImmediate(async () => {
      try {
        const { runAgentForConversation } = await import('./src/server/agent');
        const agentResult = await runAgentForConversation(activeConvId);
        if (agentResult.executed) {
          console.log(`[Agent Autoreply Sent] for conversation ${activeConvId}`);
        } else {
          console.log(`[Agent Skipped]: ${agentResult.reason}`);
        }
      } catch (err: any) {
        console.error('[Agent Background Execution Error]:', err.message);
      }
    });

    // 6. Return 200 OK immediately within budget
    return res.status(200).json({
      status: 'success',
      event_id: eventId,
      conversation_id: conversation.id,
    });
  });

  // Safety net endpoint to view webhook events
  app.get('/api/webhooks/zernio', (_req, res) => {
    const events = db.getWebhookEvents();
    res.json({ events, total: events.length });
  });

  // =========================================================================
  // FASE 4: INBOX & CONVERSATION API ROUTES
  // =========================================================================

  // List all conversations
  app.get('/api/conversations', (req, res) => {
    const channel = req.query.channel as Channel | undefined;
    const conversations = db.listConversations(channel);
    res.json({ conversations });
  });

  // Get specific conversation with message thread
  app.get('/api/conversations/:id', (req, res) => {
    const conv = db.getConversation(req.params.id);
    if (!conv) {
      return res.status(404).json({ error: 'Conversation not found' });
    }
    const messages = db.getMessagesForConversation(conv.id);
    db.markConversationRead(conv.id);
    res.json({ conversation: conv, messages });
  });

  // Get analytics summary
  app.get('/api/analytics', (_req, res) => {
    const summary = db.getAnalyticsSummary();
    res.json(summary);
  });

  // Update conversation metadata (lead stage, tags, notes)
  app.patch('/api/conversations/:id/metadata', (req, res) => {
    const updated = db.updateConversationMetadata(req.params.id, req.body);
    if (!updated) {
      return res.status(404).json({ error: 'Conversation not found' });
    }
    res.json({ conversation: updated });
  });

  // Toggle AI switch for a specific conversation (Human Agent takeover)
  app.post('/api/conversations/:id/toggle-ai', (req, res) => {
    const { enabled } = req.body;
    const updated = db.toggleConversationAi(req.params.id, Boolean(enabled));
    if (!updated) {
      return res.status(404).json({ error: 'Conversation not found' });
    }
    res.json({ conversation: updated });
  });

  // SINGLE OUTBOUND SEND ENDPOINT: POST /api/messages/send
  app.post('/api/messages/send', async (req, res) => {
    const { conversationId, text, type, templateName, templateParams, mediaUrl, senderName } = req.body;

    if (!conversationId) {
      return res.status(400).json({ error: 'conversationId is required' });
    }

    const result = await deliverMessage({
      conversationId,
      text,
      type,
      templateName,
      templateParams,
      mediaUrl,
      senderName,
      isAiGenerated: false,
    });

    if (!result.success) {
      return res.status(400).json({ error: result.error, message: result.message });
    }

    res.json({ success: true, message: result.message });
  });

  // =========================================================================
  // FASE 5: AGENT CONFIGURATION & PLAYGROUND API
  // =========================================================================

  // Get agent configs
  app.get('/api/agent/configs', (_req, res) => {
    const configs = db.getAllAgentConfigs();
    res.json({ configs });
  });

  // Update agent config for a channel
  app.put('/api/agent/configs/:channel', (req, res) => {
    const channel = req.params.channel as Channel | 'global';
    const updated = db.updateAgentConfig(channel, req.body);
    res.json({ config: updated });
  });

  // Test agent prompt simulation endpoint
  app.post('/api/agent/test-prompt', async (req, res) => {
    const { channel, message, systemPrompt } = req.body;
    try {
      const apiKey = process.env.GEMINI_API_KEY;
      if (apiKey) {
        const { GoogleGenAI } = await import('@google/genai');
        const ai = new GoogleGenAI({ apiKey });
        const resp = await ai.models.generateContent({
          model: 'gemini-3.7-flash',
          contents: [
            {
              role: 'user',
              parts: [{ text: `${systemPrompt}\n\n[Canal: ${channel}]\n\nMensaje del cliente: "${message}"` }],
            },
          ],
        });
        return res.json({ reply: resp.text || 'Sin respuesta' });
      } else {
        return res.json({
          reply: `[Simulación Local NovaBot]: Hola! Gracias por contactarnos por ${channel}. Atendemos tu consulta sobre "${message}" enseguida.`,
        });
      }
    } catch (err: any) {
      return res.status(500).json({ error: err.message });
    }
  });

  // =========================================================================
  // FASE 6: ACCOUNTS, TEMPLATES, & WEBHOOK SIMULATOR
  // =========================================================================

  app.get('/api/accounts', (_req, res) => {
    const accounts = db.getChannelAccounts();
    res.json({ accounts });
  });

  app.post('/api/accounts/toggle', (req, res) => {
    const { accountId, status } = req.body;
    db.updateAccountStatus(accountId, status);
    res.json({ success: true, accounts: db.getChannelAccounts() });
  });

  app.get('/api/templates', (_req, res) => {
    const templates = db.getTemplates();
    res.json({ templates });
  });

  // Interactive Inbound Message Simulator for UI testing
  app.post('/api/simulate/inbound', async (req, res) => {
    const { channel, senderName, senderHandle, messageText, adCampaign, isDuplicateTest } = req.body;

    const eventId = isDuplicateTest ? 'evt_duplicate_test_key_101' : `evt_sim_${Date.now()}_${Math.random().toString(36).substring(2, 5)}`;
    const phone = channel === 'whatsapp' ? senderHandle || '+52 55 9988 7766' : undefined;

    const payload = {
      event_id: eventId,
      event_type: 'message.incoming',
      channel: channel || 'whatsapp',
      provider: 'zernio',
      account_id: `acc_${channel || 'whatsapp'}_main`,
      sender: {
        id: `sim_usr_${(senderName || 'user').toLowerCase().replace(/\s+/g, '_')}`,
        name: senderName || 'Cliente Demostración',
        handle: senderHandle || `@${(senderName || 'cliente').toLowerCase().replace(/\s+/g, '')}`,
        phone,
      },
      message: {
        id: `sim_msg_${Date.now()}`,
        type: 'text',
        body: messageText || 'Hola, me interesa conocer más sobre sus productos y promociones.',
        ad_attribution: adCampaign
          ? {
              campaign_name: adCampaign,
              ad_id: `ad_${Date.now()}`,
              source: 'Meta Ads Manager (Instagram/Facebook)',
            }
          : undefined,
      },
    };

    // Forward to internal webhook flow
    const isFirstClaim = db.claimWebhookEvent({
      event_id: eventId,
      provider: 'zernio',
      channel: payload.channel as Channel,
      event_type: 'message.incoming',
      payload,
    });

    if (!isFirstClaim) {
      return res.json({
        success: true,
        idempotency_result: 'DUPLICATE_EVENT_BLOCKED (ON CONFLICT DO NOTHING)',
        event_id: eventId,
      });
    }

    const contact = db.resolveContact({
      channel: payload.channel as Channel,
      externalId: payload.sender.id,
      name: payload.sender.name,
      handle: payload.sender.handle,
      phone: payload.sender.phone,
    });

    const threadExternalId = `thread_${payload.channel}_${payload.sender.id}`;
    let conversation = db.findConversationByProviderExternalId('zernio', threadExternalId);
    const nowIso = new Date().toISOString();

    if (!conversation) {
      conversation = db.upsertConversation({
        id: `conv_${payload.channel}_${Date.now().toString(36)}`,
        channel: payload.channel as Channel,
        provider: 'zernio',
        external_id: threadExternalId,
        account_id: payload.account_id,
        participant_id: payload.sender.id,
        participant_name: contact.name,
        participant_handle: payload.sender.handle,
        participant_picture: contact.avatar_url,
        last_message_at: nowIso,
        last_inbound_at: nowIso,
        unread_count: 1,
        ai_enabled: true,
        contact_id: contact.id,
        last_message_preview: payload.message.body,
        metadata: {
          ad_attribution: payload.message.ad_attribution,
          lead_status: 'new',
          tags: [payload.channel.toUpperCase(), 'Simulated'],
        },
      });
    }

    db.insertMessage({
      conversation_id: conversation.id,
      channel: payload.channel as Channel,
      provider: 'zernio',
      external_id: payload.message.id,
      direction: 'inbound',
      type: 'text',
      body: payload.message.body,
      status: 'delivered',
      raw_payload: payload,
      sent_at: nowIso,
      sender_name: contact.name,
    });

    // Run agent
    const targetConvId = conversation.id;
    let agentResult: any = null;
    try {
      const { runAgentForConversation } = await import('./src/server/agent');
      agentResult = await runAgentForConversation(targetConvId);
    } catch (e: any) {
      console.error(e);
    }

    res.json({
      success: true,
      event_id: eventId,
      conversation_id: conversation.id,
      agent_result: agentResult,
    });
  });

  // =========================================================================
  // VITE DEV MIDDLEWARE / STATIC ASSETS
  // =========================================================================
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (_req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Multichannel CRM Server running on port ${PORT}`);
  });
}

startServer();
