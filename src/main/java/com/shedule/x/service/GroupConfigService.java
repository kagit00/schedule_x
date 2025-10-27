package com.shedule.x.service;

import com.shedule.x.exceptions.BadRequestException;
import com.shedule.x.models.MatchingGroup;
import com.shedule.x.repo.MatchingGroupRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class GroupConfigService {
    private final MatchingGroupRepository groupConfigRepository;

    @Cacheable(value = "groupCache", key = "'group_id' + '_' + #groupId + 'domain_id_' + #domainId")
    public MatchingGroup getGroupConfig(String groupId, UUID domainId) {
        return groupConfigRepository.byDomainIdAndGroupId(domainId, groupId).orElseThrow(
                () -> new BadRequestException("No group against groupId: " + groupId)
        );
    }

    @Cacheable(value = "groupCache", key = "'group_id' + '_' + #groupId + 'domain_id_' + #domainId")
    public MatchingGroup getGroupConfig(UUID groupId, UUID domainId) {
        return groupConfigRepository.findByDomainIdAndId(domainId, groupId);
    }
}
