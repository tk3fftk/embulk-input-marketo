package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.CsvTokenizer;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.bulk_extract.AllStringJacksonServiceRecord;
import org.embulk.input.marketo.bulk_extract.CsvRecordIterator;
import org.embulk.input.marketo.bulk_extract.ProgramMembersLineDecoderIterator;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.text.LineDecoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.embulk.input.marketo.MarketoInputPlugin.CONFIG_MAPPER_FACTORY;

public class ProgramMembersBulkExtractInputPlugin extends MarketoBaseInputPluginDelegate<ProgramMembersBulkExtractInputPlugin.PluginTask>
{
    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask, CsvTokenizer.PluginTask
    {
        @Config("program_ids")
        @ConfigDefault("null")
        Optional<String> getProgramIds();

        @Config("polling_interval_second")
        @ConfigDefault("60")
        Integer getPollingIntervalSecond();

        @Config("bulk_job_timeout_second")
        @ConfigDefault("3600")
        Integer getBulkJobTimeoutSecond();

        Map<String, String> getProgramMemberFields();
        void setProgramMemberFields(Map<String, String> programMemberFields);

        List<Integer> getExtractedPrograms();
        void setExtractedPrograms(List<Integer> extractedPrograms);
    }

    @Override
    public void validateInputTask(PluginTask task)
    {
        super.validateInputTask(task);
        try (MarketoRestClient marketoRestClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(marketoRestClient);
            ObjectNode result = marketoService.describeProgramMembers();
            JsonNode fields = result.get("fields");
            if (!fields.isArray()) {
                throw new DataException("[fields] isn't array node.");
            }
            Map<String, String> extractFields = new HashMap<>();
            for (JsonNode field : fields) {
                String dataType = field.get("dataType").asText();
                String name = field.get("name").asText();
                if (!extractFields.containsKey(name)) {
                    extractFields.put(name, dataType);
                }
            }
            task.setProgramMemberFields(extractFields);

            Iterable<ObjectNode> programsToRequest;
            List<Integer> programIds = new ArrayList<>();
            if (task.getProgramIds().isPresent() && StringUtils.isNotBlank(task.getProgramIds().get())) {
                final String[] idsStr = StringUtils.split(task.getProgramIds().get(), ID_LIST_SEPARATOR_CHAR);
                java.util.function.Function<Set<String>, Iterable<ObjectNode>> getListIds = marketoService::getProgramsByIds;
                programsToRequest = super.getObjectsByIds(idsStr, getListIds);
            }
            else {
                programsToRequest = marketoService.getPrograms();
            }
            Iterator<ObjectNode> iterator = programsToRequest.iterator();
            while (iterator.hasNext()) {
                ObjectNode program = iterator.next();
                int id = program.get("id").asInt();
                if (!programIds.contains(id)) {
                    programIds.add(id);
                }
            }
            if (programIds.size() <= 0) {
                throw new DataException("Cannot find any programs belong to this account.");
            }
            task.setExtractedPrograms(programIds);
        }
    }

    @Override
    public ConfigDiff buildConfigDiff(PluginTask task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public TaskReport ingestServiceData(final PluginTask task, RecordImporter recordImporter, int taskIndex, PageBuilder pageBuilder)
    {
        TaskReport taskReport = CONFIG_MAPPER_FACTORY.newTaskReport();
        if (Exec.isPreview()) {
            return MarketoUtils.importMockPreviewData(pageBuilder, PREVIEW_RECORD_LIMIT);
        }
        else {
            try (ProgramMembersLineDecoderIterator decoderIterator = getLineDecoderIterator(task, task.getExtractedPrograms().iterator())) {
                Iterator<Map<String, String>> csvRecords = Iterators.concat(Iterators.transform(decoderIterator,
                        (Function<LineDecoder, Iterator<Map<String, String>>>) input -> new CsvRecordIterator(input, task)));
                //Keep the preview code here when we can enable real preview
                if (Exec.isPreview()) {
                    csvRecords = Iterators.limit(csvRecords, PREVIEW_RECORD_LIMIT);
                }
                int imported = 0;
                while (csvRecords.hasNext()) {
                    Map<String, String> csvRecord = csvRecords.next();
                    ObjectNode objectNode = MarketoUtils.OBJECT_MAPPER.valueToTree(csvRecord);
                    recordImporter.importRecord(new AllStringJacksonServiceRecord(objectNode), pageBuilder);
                    imported = imported + 1;
                }
                return taskReport;
            }
        }
    }

    private ProgramMembersLineDecoderIterator getLineDecoderIterator(PluginTask task, Iterator<Integer> programIds)
    {
        return new ProgramMembersLineDecoderIterator(programIds, task, createMarketoRestClient(task));
    }

    @Override
    protected final Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(ProgramMembersBulkExtractInputPlugin.PluginTask task)
    {
        List<MarketoField> programMembersColumns = new ArrayList<>();
        for (Map.Entry<String, String> entry : task.getProgramMemberFields().entrySet()) {
            programMembersColumns.add(new MarketoField(entry.getKey(), entry.getValue()));
        }
        return MarketoUtils.buildDynamicResponseMapper(task.getSchemaColumnPrefix(), programMembersColumns);
    }
}
